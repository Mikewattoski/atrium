package processor

import (
	"errors"
	"testing"
	"time"

	"github.com/Mikewattoski/atrium/internal/model"
)

var t0 = time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)

func ev(dev, metric string, v float64, offset time.Duration) model.ReadingEvent {
	return model.ReadingEvent{DeviceID: dev, Metric: metric, Value: v, TS: t0.Add(offset)}
}

func TestEvaluate(t *testing.T) {
	tests := []struct {
		name   string
		events []model.ReadingEvent
		want   []AlertAction // in device-id order
	}{
		{"below threshold resolves", []model.ReadingEvent{ev("a", "temperature", 22, 0)}, []AlertAction{ResolveAlert}},
		{"above threshold opens", []model.ReadingEvent{ev("a", "temperature", 35, 0)}, []AlertAction{OpenAlert}},
		{"exactly at threshold is not an alert", []model.ReadingEvent{ev("a", "temperature", 30, 0)}, []AlertAction{ResolveAlert}},
		{
			"newest reading wins (hot then cool)",
			[]model.ReadingEvent{ev("a", "temperature", 35, 0), ev("a", "temperature", 22, time.Second)},
			[]AlertAction{ResolveAlert},
		},
		{
			"newest reading wins even if it arrives first",
			[]model.ReadingEvent{ev("a", "temperature", 22, time.Second), ev("a", "temperature", 35, 0)},
			[]AlertAction{ResolveAlert},
		},
		{
			"other metrics are ignored",
			[]model.ReadingEvent{ev("a", "humidity", 99, 0)},
			[]AlertAction{},
		},
		{
			"multiple devices are sorted by id",
			[]model.ReadingEvent{ev("b", "temperature", 40, 0), ev("a", "temperature", 20, 0)},
			[]AlertAction{ResolveAlert, OpenAlert},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Evaluate(tc.events, 30)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d decisions, want %d: %+v", len(got), len(tc.want), got)
			}
			for i := range got {
				if got[i].Action != tc.want[i] {
					t.Errorf("decision %d (%s) action = %v, want %v", i, got[i].DeviceID, got[i].Action, tc.want[i])
				}
			}
		})
	}
}

func TestDecode(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{"valid", `{"device_id":"d1","metric":"temperature","value":21.5,"ts":"2026-10-22T12:00:00Z"}`, false},
		{"not json", `not json`, true},
		{"missing device", `{"metric":"temperature","value":1,"ts":"2026-10-22T12:00:00Z"}`, true},
		{"missing metric", `{"device_id":"d1","value":1,"ts":"2026-10-22T12:00:00Z"}`, true},
		{"missing ts", `{"device_id":"d1","metric":"temperature","value":1}`, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decode([]byte(tc.body))
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr && !errors.Is(err, ErrBadMessage) {
				t.Errorf("want ErrBadMessage, got %v", err)
			}
		})
	}
}
