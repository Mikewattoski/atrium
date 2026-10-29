package processor

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Mikewattoski/atrium/internal/model"
)

var ErrBadMessage = errors.New("bad message")

// Decode parses and validates one queue message body.
func Decode(body []byte) (model.ReadingEvent, error) {
	var ev model.ReadingEvent
	if err := json.Unmarshal(body, &ev); err != nil {
		return ev, fmt.Errorf("%w: %v", ErrBadMessage, err)
	}
	if ev.DeviceID == "" || ev.Metric == "" {
		return ev, fmt.Errorf("%w: device_id and metric are required", ErrBadMessage)
	}
	if ev.TS.IsZero() {
		return ev, fmt.Errorf("%w: ts is required", ErrBadMessage)
	}
	return ev, nil
}
