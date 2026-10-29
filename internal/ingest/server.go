package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Mikewattoski/atrium/internal/model"
	telemetryv1 "github.com/Mikewattoski/atrium/proto/telemetry/v1"
)

const maxFutureSkew = 5 * time.Minute

type Publisher interface {
	Publish(ctx context.Context, routingKey string, body []byte) error
}

type Server struct {
	telemetryv1.UnimplementedTelemetryServiceServer
	pub Publisher
	now func() time.Time
}

func NewServer(pub Publisher) *Server {
	return &Server{pub: pub, now: time.Now}
}

// StreamReadings validates and publishes every reading in the stream.
// Invalid readings are counted as rejected; a broker failure aborts the
// stream with Unavailable so the client can reconnect and retry.
func (s *Server) StreamReadings(stream telemetryv1.TelemetryService_StreamReadingsServer) error {
	var accepted, rejected int64

	for {
		r, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&telemetryv1.Ack{Accepted: accepted, Rejected: rejected})
		}
		if err != nil {
			return err
		}

		ev, err := s.validate(r)
		if err != nil {
			rejected++
			continue
		}

		body, err := json.Marshal(ev)
		if err != nil {
			return status.Errorf(codes.Internal, "marshal: %v", err)
		}
		key := "readings." + strings.ReplaceAll(ev.DeviceID, ".", "_")
		if err := s.pub.Publish(stream.Context(), key, body); err != nil {
			return status.Errorf(codes.Unavailable, "publish: %v", err)
		}
		accepted++
	}
}

func (s *Server) validate(r *telemetryv1.Reading) (model.ReadingEvent, error) {
	id := strings.TrimSpace(r.GetDeviceId())
	metric := strings.TrimSpace(r.GetMetric())

	switch {
	case id == "" || len(id) > 64:
		return model.ReadingEvent{}, errors.New("device_id must be 1-64 chars")
	case metric == "" || len(metric) > 64:
		return model.ReadingEvent{}, errors.New("metric must be 1-64 chars")
	case math.IsNaN(r.GetValue()) || math.IsInf(r.GetValue(), 0):
		return model.ReadingEvent{}, errors.New("value must be a finite number")
	case r.GetTs() == nil:
		return model.ReadingEvent{}, errors.New("ts is required")
	}
	if err := r.GetTs().CheckValid(); err != nil {
		return model.ReadingEvent{}, err
	}
	ts := r.GetTs().AsTime().UTC()
	if ts.After(s.now().Add(maxFutureSkew)) {
		return model.ReadingEvent{}, errors.New("ts is too far in the future")
	}

	return model.ReadingEvent{DeviceID: id, Metric: metric, Value: r.GetValue(), TS: ts}, nil
}
