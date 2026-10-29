.RECIPEPREFIX = >
include .env
export

.PHONY: up down reset logs ps migrate seed psql test lint

up:
> docker compose up -d
> docker compose logs migrate

down:
> docker compose down

reset:
> docker compose down -v

logs:
> docker compose logs -f

ps:
> docker compose ps

migrate:
> docker compose run --rm migrate

seed:
> docker compose exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) < seeds/seed.sql

psql:
> docker compose exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

test:
> go test -race ./...

lint:
> golangci-lint run ./...

run-api:
> go run ./cmd/api

.PHONY: proto run-api run-ingest run-simulator seed-devices

proto:
> protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/telemetry/v1/telemetry.proto

run-ingest:
> go run ./cmd/ingest

run-simulator:
> go run ./cmd/simulator

seed-devices:
> docker compose exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c "INSERT INTO devices (id, name, type, location) SELECT 'dev-' || lpad(i::text, 3, '0'), 'Simulated Sensor ' || i, 'temperature_sensor', 'Floor ' || ((i - 1) / 20 + 1) FROM generate_series(1, 100) AS i ON CONFLICT (id) DO NOTHING;"

.PHONY: run-processor stats

run-processor:
> go run ./cmd/processor

stats:
> docker compose exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c "SELECT (SELECT count(*) FROM readings) AS readings, (SELECT count(*) FROM alerts WHERE status = 'open') AS open_alerts, (SELECT count(*) FROM alerts) AS total_alerts;"
> docker compose exec rabbitmq rabbitmqctl list_queues name messages consumers
