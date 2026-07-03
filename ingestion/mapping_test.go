package main

import (
	"testing"

	gtfsrt "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
)

func f32(v float32) *float32 { return &v }

func TestMapVehiclePositions(t *testing.T) {
	feed := &gtfsrt.FeedMessage{
		Entity: []*gtfsrt.FeedEntity{
			{
				Id: protoString("1"),
				Vehicle: &gtfsrt.VehiclePosition{
					Trip: &gtfsrt.TripDescriptor{
						TripId:  protoString("trip-1"),
						RouteId: protoString("route-1"),
					},
					Vehicle: &gtfsrt.VehicleDescriptor{
						Id: protoString("vehicle-1"),
					},
					Position: &gtfsrt.Position{
						Latitude:  f32(54.35),
						Longitude: f32(18.65),
						Bearing:   f32(90),
						Speed:     f32(12.5),
					},
					Timestamp: uint64Ptr(1700000000),
				},
			},
			{
				// No bearing/speed reported -- must map to null, not zero.
				Id: protoString("2"),
				Vehicle: &gtfsrt.VehiclePosition{
					Trip: &gtfsrt.TripDescriptor{
						TripId:  protoString("trip-2"),
						RouteId: protoString("route-2"),
					},
					Vehicle: &gtfsrt.VehicleDescriptor{
						Id: protoString("vehicle-2"),
					},
					Position: &gtfsrt.Position{
						Latitude:  f32(54.36),
						Longitude: f32(18.66),
					},
					Timestamp: uint64Ptr(1700000060),
				},
			},
			{
				// No position at all -- must be skipped entirely.
				Id: protoString("3"),
				Vehicle: &gtfsrt.VehiclePosition{
					Trip: &gtfsrt.TripDescriptor{
						TripId:  protoString("trip-3"),
						RouteId: protoString("route-3"),
					},
				},
			},
			{
				// Non-vehicle entity (e.g. alert-only) -- must be skipped.
				Id: protoString("4"),
			},
		},
	}

	records := MapVehiclePositions(feed, 7)

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	r0 := records[0]
	if r0.VehicleID != "vehicle-1" || r0.RouteID != "route-1" || r0.TripID != "trip-1" {
		t.Errorf("unexpected identity fields: %+v", r0)
	}
	if r0.Latitude != float64(float32(54.35)) || r0.Longitude != float64(float32(18.65)) {
		t.Errorf("unexpected coordinates: %+v", r0)
	}
	if r0.Bearing == nil || *r0.Bearing != float64(float32(90)) {
		t.Errorf("expected bearing 90, got %v", r0.Bearing)
	}
	if r0.SpeedMPS == nil || *r0.SpeedMPS != float64(float32(12.5)) {
		t.Errorf("expected speed 12.5, got %v", r0.SpeedMPS)
	}
	if r0.TimestampUTC != "2023-11-14T22:13:20Z" {
		t.Errorf("unexpected timestamp: %s", r0.TimestampUTC)
	}
	if r0.FeedSequence != 7 {
		t.Errorf("expected feed_sequence 7, got %d", r0.FeedSequence)
	}

	r1 := records[1]
	if r1.Bearing != nil {
		t.Errorf("expected nil bearing, got %v", *r1.Bearing)
	}
	if r1.SpeedMPS != nil {
		t.Errorf("expected nil speed, got %v", *r1.SpeedMPS)
	}
}

func TestMapVehiclePositionsEmptyFeed(t *testing.T) {
	records := MapVehiclePositions(&gtfsrt.FeedMessage{}, 1)
	if len(records) != 0 {
		t.Fatalf("expected 0 records for empty feed, got %d", len(records))
	}
}

func protoString(s string) *string { return &s }
func uint64Ptr(v uint64) *uint64   { return &v }
