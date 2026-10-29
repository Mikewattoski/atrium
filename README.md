# Atrium

**Real-time telemetry for smart buildings.**

Atrium collects readings from building sensors (temperature, HVAC), pushes them through a reliable message pipeline, stores them in PostgreSQL, raises alerts when a value goes out of range, and exposes everything through a REST API.

It is written in Go and designed around the problems real telemetry systems face: bursts of data, a slow or unavailable database, duplicate deliveries and bad messages.

> The devices are simulated, "BACnet-style" sensors (device IDs, metrics, readings). Atrium does not implement the BACnet/IP protocol itself.

## Why this project exists

A large building has hundreds of devices sending a reading every few seconds. Someone has to:

1. receive those readings reliably, even in bursts,
2. store them so the history can be queried,
3. warn people when something is wrong (for example, a server room above 30 °C),
4. let other systems ask "what is the current temperature in room X?".

Atrium is a compact, end-to-end implementation of that pipeline, with every component small enough to read in one sitting.

## Architecture

```mermaid
flowchart LR
    SIM["Simulator<br/>100 devices"] -- "gRPC client stream" --> ING["Ingest service<br/>validation"]
    ING -- "publish + confirm" --> MQ[("RabbitMQ<br/>readings.q")]
    MQ -- "manual ack" --> PROC["Processor<br/>batching + alerts"]
    MQ -. "failed messages" .-> DLQ[("readings.dlq")]
    PROC -- "batch insert" --> PG[("PostgreSQL")]
    API["REST API<br/>Gin"] -- "queries" --> PG
    USER["curl / dashboard"] --> API
```

| Component | Responsibility |
|---|---|
| **Simulator** | 100 goroutines, one per device, producing a temperature reading every second (random walk with occasional spikes). A single sender goroutine owns the gRPC stream. |
| **Ingest** | gRPC server. Validates every reading, publishes valid ones to RabbitMQ with publisher confirms, and reports accepted/rejected counts. |
| **RabbitMQ** | Durable buffer between ingest and storage. A topic exchange feeds `readings.q`, with a dead-letter exchange and queue for failures. |
| **Processor** | Consumes in batches with manual acks, writes to PostgreSQL idempotently, and opens or resolves alerts. |
| **PostgreSQL** | Devices, readings and alerts, with indexes for time-ordered queries. |
| **REST API** | Devices, reading history (cursor pagination), latest reading and alerts. |

## Key design decisions

**At-least-once delivery + idempotent writes.** Messages are acknowledged only after they are stored. If the processor crashes mid-batch, RabbitMQ redelivers, and a unique constraint on `(device_id, metric, ts)` with `ON CONFLICT DO NOTHING` makes the replay harmless.

**Batched inserts.** The processor collects up to 500 messages (or 1 second) and writes them in a single `unnest` statement instead of 500 round trips.

**Poison-message isolation.** Malformed messages go straight to the dead-letter queue. If a batch is rejected because of its data (for example, an unknown device), the processor falls back to per-message inserts so one bad message cannot block the good ones. Transient errors (database down) requeue the messages and retry.

**Backpressure at every hop.** RabbitMQ prefetch bounds in-flight messages. The simulator blocks when ingest is slow. Ingest returns `Unavailable` when the broker is down, and the client reconnects.

**Race-free alerting.** A partial unique index allows at most one *open* alert per device and metric, so duplicate alerts are impossible even with concurrent consumers. The alert logic is a pure function and is unit-tested separately.

**Client streaming gRPC.** One long-lived stream per simulator, instead of one RPC per reading, cuts overhead for high-frequency telemetry.

**Keyset pagination.** The readings endpoint pages by timestamp cursor instead of `OFFSET`, so deep pages stay fast and use the `(device_id, ts DESC)` index.

**Graceful shutdown everywhere.** `SIGINT`/`SIGTERM` drain in-flight work: the processor writes and acknowledges its current batch before exiting.

## Tech stack

Go · gRPC / Protocol Buffers · RabbitMQ · PostgreSQL (pgx) · Gin · Docker Compose · golang-migrate

## Quickstart

**Prerequisites:** Go (64-bit), Docker with Compose, `make`, and `protoc` only if you change the `.proto` file.

```bash
git clone https://github.com/Mikewattoski/atrium.git
cd atrium
cp .env.example .env

make up             # PostgreSQL, Redis, RabbitMQ + database migrations
make seed-devices   # register the 100 simulated devices
```

Run each service in its own terminal:

```bash
make run-processor  # consumes readings and writes to PostgreSQL
make run-ingest     # gRPC server on :9090
make run-simulator  # 100 devices, one reading per second each
make run-api        # REST API on :8080
```

Check that data is flowing:

```bash
make stats          # row counts and queue depth
```

RabbitMQ management UI: <http://localhost:15672> (login `guest` / `guest`).

## REST API

Base path: `/api/v1`

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Liveness and database check |
| GET | `/devices` | List devices |
| GET | `/devices/{id}/latest?metric=` | Most recent reading |
| GET | `/devices/{id}/readings?metric=&from=&to=&limit=&cursor=` | History, newest first, cursor-paginated |
| GET | `/alerts?status=open\|resolved&limit=` | Alerts |

```bash
curl -s localhost:8080/api/v1/devices/dev-001/latest
# {"id":1042,"device_id":"dev-001","metric":"temperature","value":22.7,"ts":"2026-10-22T12:00:01Z"}

curl -s "localhost:8080/api/v1/devices/dev-001/readings?limit=2"
# {"items":[...],"next_cursor":"2026-10-22T11:59:59.5Z"}

curl -s "localhost:8080/api/v1/alerts?status=open"
```

Times are RFC 3339. Pass `next_cursor` back as `cursor` to fetch the next page.

## Configuration

All settings are environment variables with local defaults.

| Variable | Default | Used by |
|---|---|---|
| `DATABASE_URL` | `postgres://bp:bp_pass@localhost:5432/buildingpulse?sslmode=disable` | api, processor |
| `AMQP_URL` | `amqp://guest:guest@localhost:5672/` | ingest, processor |
| `HTTP_ADDR` | `:8080` | api |
| `GRPC_ADDR` | `:9090` | ingest |
| `BATCH_SIZE` | `500` | processor |
| `FLUSH_INTERVAL` | `1s` | processor |
| `ALERT_TEMP_THRESHOLD` | `30` | processor |

Example: `ALERT_TEMP_THRESHOLD=25 make run-processor`

## Testing

```bash
make test           # go test -race ./...
```

Unit tests cover the service layer (pagination, validation), the ingest validation and routing, the alert decision logic, message decoding, and the processor's ack/nack behaviour for the happy path, malformed messages, unknown devices, database outages and alert failures. The processor is tested against fakes, so no RabbitMQ or PostgreSQL is needed.

## Failure behaviour

| Situation | Result |
|---|---|
| PostgreSQL goes down | Messages are requeued, the queue grows, and the backlog drains after recovery. No data is lost. |
| Processor crashes before acking | Messages are redelivered. Idempotent inserts prevent duplicates. |
| Malformed message | Dead-lettered to `readings.dlq`. |
| Reading for an unknown device | Only that message is dead-lettered. The rest of the batch is stored. |
| RabbitMQ restarts | Ingest and the processor reconnect automatically. |
| Ingest restarts | The simulator logs the error and reconnects. |

## Project layout

```
cmd/
  api/ ingest/ processor/ simulator/   service entry points
internal/
  config/      environment configuration
  model/       domain types and queue event
  repository/  PostgreSQL queries (API side)
  service/     API business logic and validation
  handler/     HTTP handlers
  ingest/      gRPC server
  processor/   batching, alert logic, PostgreSQL store
  queue/       RabbitMQ topology, publisher, consumer
proto/         gRPC contract
migrations/    SQL schema (golang-migrate)
seeds/         sample data
deploy/k8s/    Kubernetes manifests (planned)
```

## Roadmap

- [x] PostgreSQL schema with idempotency and alert constraints
- [x] REST API with cursor pagination
- [x] gRPC ingest service and device simulator
- [x] RabbitMQ pipeline with confirms, manual acks and dead-lettering
- [x] Batch processor with alerting
- [ ] Redis cache-aside for latest readings and API rate limiting
- [ ] Grafana dashboard
- [ ] Dockerfiles for every service and full Compose stack
- [ ] GitHub Actions CI (lint, race-detector tests, image build)
- [ ] Kubernetes manifests with health probes
- [ ] Prometheus metrics

## Known limitations

- Alerts are decided from the newest reading in each batch. After a requeue, an out-of-order delivery could briefly flip an alert's state.
- A batch that contains an unknown device falls back to single inserts. A production system would cache known device IDs and filter first.
- The publisher waits for a broker confirm per message. That is simple and safe at this scale, and batching confirms is the next optimisation.
