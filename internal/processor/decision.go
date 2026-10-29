package processor

import (
	"slices"
	"strings"

	"github.com/Mikewattoski/atrium/internal/model"
)

const TemperatureMetric = "temperature"

type AlertAction int

const (
	OpenAlert AlertAction = iota + 1
	ResolveAlert
)

type AlertDecision struct {
	Action    AlertAction
	DeviceID  string
	Metric    string
	Value     float64
	Threshold float64
}

type deviceMetric struct{ device, metric string }

// Evaluate looks at the newest temperature reading per device in the batch:
// above the threshold -> open an alert, otherwise -> resolve any open alert.
// It is a pure function, so it is easy to test.
func Evaluate(events []model.ReadingEvent, threshold float64) []AlertDecision {
	latest := make(map[deviceMetric]model.ReadingEvent)
	for _, ev := range events {
		if ev.Metric != TemperatureMetric {
			continue
		}
		k := deviceMetric{ev.DeviceID, ev.Metric}
		if cur, ok := latest[k]; !ok || !ev.TS.Before(cur.TS) {
			latest[k] = ev
		}
	}

	out := make([]AlertDecision, 0, len(latest))
	for k, ev := range latest {
		d := AlertDecision{
			Action:    ResolveAlert,
			DeviceID:  k.device,
			Metric:    k.metric,
			Value:     ev.Value,
			Threshold: threshold,
		}
		if ev.Value > threshold {
			d.Action = OpenAlert
		}
		out = append(out, d)
	}
	slices.SortFunc(out, func(a, b AlertDecision) int {
		return strings.Compare(a.DeviceID, b.DeviceID)
	})
	return out
}
