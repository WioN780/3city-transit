package main

import (
	"time"

	gtfsrt "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
)

// GPSPosition mirrors the gps_raw Kafka schema (see docs/architecture.md §3.1).
type GPSPosition struct {
	VehicleID    string   `json:"vehicle_id"`
	RouteID      string   `json:"route_id"`
	TripID       string   `json:"trip_id"`
	Latitude     float64  `json:"latitude"`
	Longitude    float64  `json:"longitude"`
	Bearing      *float64 `json:"bearing"`
	SpeedMPS     *float64 `json:"speed_mps"`
	TimestampUTC string   `json:"timestamp_utc"`
	FeedSequence int      `json:"feed_sequence"`
}

// MapVehiclePositions converts a decoded GTFS-RT FeedMessage into gps_raw
// records. feedSequence is stamped onto every record produced by this poll --
// the GTFS-RT spec has no native per-entity sequence number, so this counts
// which poll of the upstream feed the record came from.
func MapVehiclePositions(feed *gtfsrt.FeedMessage, feedSequence int) []GPSPosition {
	var out []GPSPosition
	for _, entity := range feed.GetEntity() {
		vp := entity.GetVehicle()
		if vp == nil {
			continue
		}
		pos := vp.GetPosition()
		if pos == nil || pos.Latitude == nil || pos.Longitude == nil {
			continue
		}

		record := GPSPosition{
			VehicleID:    vp.GetVehicle().GetId(),
			RouteID:      vp.GetTrip().GetRouteId(),
			TripID:       vp.GetTrip().GetTripId(),
			Latitude:     float64(*pos.Latitude),
			Longitude:    float64(*pos.Longitude),
			TimestampUTC: time.Unix(int64(vp.GetTimestamp()), 0).UTC().Format(time.RFC3339),
			FeedSequence: feedSequence,
		}
		if pos.Bearing != nil {
			v := float64(*pos.Bearing)
			record.Bearing = &v
		}
		if pos.Speed != nil {
			v := float64(*pos.Speed)
			record.SpeedMPS = &v
		}
		out = append(out, record)
	}
	return out
}
