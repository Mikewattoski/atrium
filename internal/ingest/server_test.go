package ingest

import (
	"context"
	"errors"
	"io"
	"math"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	telemetryv1 "github.com/Mikewattoski/atrium/proto/telemetry/v1"
)

type fakePub struct {
	keys []string
	err  error
}

func (f *fakePub) Publish(_ context.Context, key string, _ []byte) error {
	if f.err != nil {
		return f.err
	}
	f.keys = append(f.keys, key)
	return nil
}

type fakeStream struct {
	grpc.ServerStream
	in  []*telemetryv1.Reading
	i   int
	ack *telemetryv1.Ack
}

func (f *fakeStream) Context() context.Context { return context.Background() }
func (f *fakeStream) Recv() (*telemetryv1.Reading, error) {
	if f.i >= len(f.in) {
		return nil, io.EOF
	}
	r := f.in[f.i]
	f.i++
	return r, nil
}
func (f *fakeStream) SendAndClose(a *telemetryv1.Ack) error { f.ack = a; return nil }

func rd(id, metric string, v float64, ts time.Time) *telemetryv1.Reading {
	return &telemetryv1.Reading{DeviceId: id, Metric: metric, Value: v, Ts: timestamppb.New(ts)}
}

func TestStreamReadings(t *testing.T) {
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	good := rd("dev-001", "temperature", 22.5, now)

	tests := []struct {
		name         string
		in           []*telemetryv1.Reading
		pubErr       error
		wantAccepted int64
		wantRejected int64
		wantCode     codes.Code
	}{
		{
			name:         "all valid",
			in:           []*telemetryv1.Reading{good, good},
			wantAccepted: 2,
			wantCode:     codes.OK,
		},
		{
			name: "invalid readings are rejected, stream continues",
			in: []*telemetryv1.Reading{
				good,
				rd("", "temperature", 1, now),          // empty device
				rd("d", "m", math.NaN(), now),          // NaN value
				{DeviceId: "d", Metric: "m", Value: 1}, // missing ts
				rd("d", "m", 1, now.Add(time.Hour)),    // future ts
			},
			wantAccepted: 1,
			wantRejected: 4,
			wantCode:     codes.OK,
		},
		{
			name:     "broker failure returns Unavailable",
			in:       []*telemetryv1.Reading{good},
			pubErr:   errors.New("boom"),
			wantCode: codes.Unavailable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewServer(&fakePub{err: tc.pubErr})
			srv.now = func() time.Time { return now }
			fs := &fakeStream{in: tc.in}

			err := srv.StreamReadings(fs)

			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("code = %v, want %v (err=%v)", got, tc.wantCode, err)
			}
			if tc.wantCode != codes.OK {
				return
			}
			if fs.ack.Accepted != tc.wantAccepted || fs.ack.Rejected != tc.wantRejected {
				t.Errorf("ack = %d/%d, want %d/%d",
					fs.ack.Accepted, fs.ack.Rejected, tc.wantAccepted, tc.wantRejected)
			}
		})
	}
}

func TestRoutingKeyReplacesDots(t *testing.T) {
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	pub := &fakePub{}
	srv := NewServer(pub)
	srv.now = func() time.Time { return now }

	fs := &fakeStream{in: []*telemetryv1.Reading{rd("floor.1.dev", "temperature", 20, now)}}
	if err := srv.StreamReadings(fs); err != nil {
		t.Fatal(err)
	}
	if len(pub.keys) != 1 || pub.keys[0] != "readings.floor_1_dev" {
		t.Errorf("routing keys = %v", pub.keys)
	}
}
