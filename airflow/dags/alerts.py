"""Shared failure alerting, used as on_failure_callback across every DAG in
this package.

Posts to Slack via the `slack_webhook_default` connection when one is
configured; otherwise falls back to a loud ERROR-level log line so a
failure is never silent even before a real Slack workspace is wired up.
"""
from __future__ import annotations

import logging

from airflow.hooks.base import BaseHook

logger = logging.getLogger("airflow.task")

SLACK_CONN_ID = "slack_webhook_default"


def on_failure_alert(context) -> None:
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    message = f":red_circle: `{dag_id}.{task_id}` failed (run {run_id}): {context.get('exception')}"

    try:
        BaseHook.get_connection(SLACK_CONN_ID)
    except Exception:
        logger.error(message)
        return

    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

    SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID).send(text=message)
