"""Periodic (5-minute) check that GPS data is actually flowing:
gps_raw poller -> Kafka -> bronze.gps_positions (Structured Streaming).
Two independent checks; either one failing is enough to alert.

1. check_poller_health -- HTTP GETs the ingestion poller's own /healthz
   (ingestion/health.go). NOTE: this is a different, real endpoint from the
   Go API's /api/v1/healthz (docs/architecture.md "Go API response shape"),
   which is a dashboard-facing aggregate and currently a stub that always
   returns {"status": "ok"}. The poller's actual shape is
   {status, last_successful_poll_utc, consecutive_failures}.

2. check_kafka_topic_activity -- uses KafkaConsumerHook to read gps_raw's
   current high-watermark offset and compares it to the value recorded on
   the previous run (an Airflow Variable). If the topic hasn't grown in
   STALE_THRESHOLD_SECONDS, no new pings have reached Kafka at all.
   This is a producer-side freshness signal (poller -> Kafka), not literal
   Kafka consumer-group lag for the bronze streaming job: Spark's Kafka
   source tracks progress in its own checkpoint rather than committing
   offsets to a Kafka consumer group, so there's no __consumer_offsets
   entry to read a real lag number from.

Requires an Airflow connection `kafka_default` (extra:
bootstrap.servers=redpanda:9092, group.id=airflow-streaming-health-check)
and `ingestion_poller` (HTTP, host=ingestion, port=8091).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
from airflow.providers.http.hooks.http import HttpHook
from confluent_kafka import TopicPartition

from alerts import on_failure_alert

STALE_THRESHOLD_SECONDS = 180  # "no new GPS data in 3 minutes" per the spec
KAFKA_TOPIC = "gps_raw"
OFFSET_WATERMARK_VARIABLE = "gps_raw_offset_watermark"


def check_poller_health(**context) -> None:
    response = HttpHook(method="GET", http_conn_id="ingestion_poller").run(endpoint="/healthz")
    body = response.json()

    status = body.get("status")
    if status != "ok":
        raise AirflowException(f"ingestion poller reports status={status!r}: {body}")

    last_poll = body.get("last_successful_poll_utc")
    if not last_poll:
        raise AirflowException(f"ingestion poller has never completed a successful poll: {body}")

    age_seconds = (
        datetime.now(timezone.utc) - datetime.fromisoformat(last_poll.replace("Z", "+00:00"))
    ).total_seconds()
    if age_seconds > STALE_THRESHOLD_SECONDS:
        raise AirflowException(
            f"ingestion poller's last successful poll was {age_seconds:.0f}s ago "
            f"(threshold {STALE_THRESHOLD_SECONDS}s): {body}"
        )


def check_kafka_topic_activity(**context) -> None:
    consumer = KafkaConsumerHook(topics=[KAFKA_TOPIC], kafka_config_id="kafka_default").get_consumer()
    try:
        metadata = consumer.list_topics(KAFKA_TOPIC, timeout=10)
        partitions = metadata.topics[KAFKA_TOPIC].partitions.keys()
        if not partitions:
            raise AirflowException(f"topic {KAFKA_TOPIC!r} has no partitions -- does it exist?")

        total_offset = sum(
            consumer.get_watermark_offsets(TopicPartition(KAFKA_TOPIC, p), timeout=10, cached=False)[1]
            for p in partitions
        )
    finally:
        consumer.close()

    now = datetime.now(timezone.utc)
    stored = Variable.get(OFFSET_WATERMARK_VARIABLE, default_var=None, deserialize_json=True)

    if stored is None or total_offset > stored["total_offset"]:
        Variable.set(
            OFFSET_WATERMARK_VARIABLE,
            {"total_offset": total_offset, "last_growth_utc": now.isoformat()},
            serialize_json=True,
        )
        return

    last_growth = datetime.fromisoformat(stored["last_growth_utc"])
    stale_seconds = (now - last_growth).total_seconds()
    if stale_seconds > STALE_THRESHOLD_SECONDS:
        raise AirflowException(
            f"gps_raw total offset stuck at {total_offset} for {stale_seconds:.0f}s "
            f"(threshold {STALE_THRESHOLD_SECONDS}s) -- no new GPS pings reaching Kafka"
        )


with DAG(
    dag_id="streaming_health_check",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["streaming", "monitoring"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": on_failure_alert,
    },
) as dag:
    poller_check = PythonOperator(task_id="check_poller_health", python_callable=check_poller_health)
    kafka_check = PythonOperator(task_id="check_kafka_topic_activity", python_callable=check_kafka_topic_activity)
