package processor

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Mikewattoski/atrium/internal/model"
)

// Msg is the small slice of amqp.Delivery the processor needs,
// so the batch logic can be tested without RabbitMQ.
type Msg interface {
	Body() []byte
	Ack() error
	Nack(requeue bool) error
}

type deliveryMsg struct{ d amqp.Delivery }

func (m deliveryMsg) Body() []byte            { return m.d.Body }
func (m deliveryMsg) Ack() error              { return m.d.Ack(false) }
func (m deliveryMsg) Nack(requeue bool) error { return m.d.Nack(false, requeue) }

type Options struct {
	BatchSize     int
	FlushInterval time.Duration
	Threshold     float64
	RetryDelay    time.Duration // pause after a transient failure (0 = none)
}

type stats struct{ stored, dead, retried, malformed int }

type item struct {
	msg Msg
	ev  model.ReadingEvent
}

type Processor struct {
	store Store
	opt   Options
	stats stats // only touched from the Run goroutine
	cache LatestWriter
}

func New(store Store, opt Options) *Processor {
	if opt.BatchSize <= 0 {
		opt.BatchSize = 500
	}
	if opt.FlushInterval <= 0 {
		opt.FlushInterval = time.Second
	}
	return &Processor{store: store, opt: opt}
}

// Run collects deliveries into batches (size or time based) and handles them.
// It returns when the channel closes or ctx is cancelled, after flushing.
func (p *Processor) Run(ctx context.Context, in <-chan amqp.Delivery) {
	batch := make([]Msg, 0, p.opt.BatchSize)

	flushTick := time.NewTicker(p.opt.FlushInterval)
	defer flushTick.Stop()
	statsTick := time.NewTicker(5 * time.Second)
	defer statsTick.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		// Detach from ctx cancellation: on shutdown the in-flight batch
		// must still be written and acked.
		wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		p.HandleBatch(wctx, batch)
		batch = batch[:0]
	}

	for {
		select {
		case d, ok := <-in:
			if !ok {
				flush()
				return
			}
			batch = append(batch, deliveryMsg{d})
			if len(batch) >= p.opt.BatchSize {
				flush()
			}
		case <-flushTick.C:
			flush()
		case <-statsTick.C:
			p.logStats()
		case <-ctx.Done():
			flush()
			return
		}
	}
}

// HandleBatch persists a batch and settles every message exactly once:
// ack (stored), nack without requeue (dead-letter) or nack with requeue (retry).
func (p *Processor) HandleBatch(ctx context.Context, msgs []Msg) {
	items := make([]item, 0, len(msgs))
	for _, m := range msgs {
		ev, err := Decode(m.Body())
		if err != nil {
			log.Printf("dead-lettering malformed message: %v", err)
			p.stats.malformed++
			logErr("nack", m.Nack(false))
			continue
		}
		items = append(items, item{msg: m, ev: ev})
	}
	if len(items) == 0 {
		return
	}

	stored, dead, retry := p.persist(ctx, items)

	// Alerts are applied BEFORE acking: if this fails the messages are
	// requeued, and the redelivery re-inserts readings as a no-op.
	if len(stored) > 0 {
		decisions := Evaluate(events(stored), p.opt.Threshold)
		if err := p.store.ApplyAlerts(ctx, decisions); err != nil {
			log.Printf("apply alerts: %v (requeueing %d messages)", err, len(stored))
			retry = append(retry, stored...)
			stored = nil
		}
	}

	for _, it := range stored {
		logErr("ack", it.msg.Ack())
	}
	for _, it := range dead {
		logErr("nack", it.msg.Nack(false))
	}
	for _, it := range retry {
		logErr("nack", it.msg.Nack(true))
	}

	p.updateCache(ctx, stored)

	p.stats.stored += len(stored)
	p.stats.dead += len(dead)
	p.stats.retried += len(retry)

	if len(retry) > 0 && p.opt.RetryDelay > 0 {
		time.Sleep(p.opt.RetryDelay) // avoid a hot redelivery loop
	}
}

// persist tries one batch insert. If the database rejects the data (for
// example an unknown device), it falls back to one-by-one inserts so a single
// bad message cannot block the good ones.
func (p *Processor) persist(ctx context.Context, items []item) (stored, dead, retry []item) {
	err := p.store.InsertReadings(ctx, events(items))
	if err == nil {
		return items, nil, nil
	}
	if !IsDataError(err) {
		log.Printf("insert batch: %v (requeueing %d messages)", err, len(items))
		return nil, nil, items
	}

	log.Printf("batch rejected (%v), falling back to per-message inserts", err)
	for _, it := range items {
		switch err := p.store.InsertReading(ctx, it.ev); {
		case err == nil:
			stored = append(stored, it)
		case IsDataError(err):
			dead = append(dead, it)
		default:
			retry = append(retry, it)
		}
	}
	return stored, dead, retry
}

func (p *Processor) logStats() {
	s := p.stats
	if s == (stats{}) {
		return
	}
	log.Printf("last 5s: stored=%d dead-lettered=%d requeued=%d malformed=%d",
		s.stored, s.dead, s.retried, s.malformed)
	p.stats = stats{}
}

func events(items []item) []model.ReadingEvent {
	out := make([]model.ReadingEvent, len(items))
	for i, it := range items {
		out[i] = it.ev
	}
	return out
}

func logErr(op string, err error) {
	if err != nil {
		log.Printf("%s failed: %v", op, err)
	}
}
