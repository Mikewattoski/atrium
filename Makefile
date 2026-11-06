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

.PHONY: redis-cli

redis-cli:
> docker compose exec redis redis-cli

.PHONY: docker-build up-all down-all logs-app

docker-build:
> docker compose --profile app build

up-all:
> docker compose --profile app up -d --build
> docker compose --profile app ps

down-all:
> docker compose --profile app down

logs-app:
> docker compose --profile app logs -f api ingest processor simulator

.PHONY: k8s-cluster k8s-load k8s-up k8s-seed k8s-sim k8s-status k8s-stats k8s-forward k8s-down

k8s-cluster:    ## create the local kind cluster
> kind create cluster --name atrium

k8s-load:       ## copy the locally built images into the cluster (run make docker-build first)
> kind load docker-image atrium-api:local atrium-ingest:local atrium-processor:local atrium-simulator:local --name atrium

k8s-up:         ## deploy infrastructure, run migrations, then the apps
> kubectl apply -f deploy/k8s/00-namespace.yaml -f deploy/k8s/01-config.yaml
> kubectl apply -f deploy/k8s/10-postgres.yaml -f deploy/k8s/11-redis.yaml -f deploy/k8s/12-rabbitmq.yaml
> kubectl -n atrium rollout status statefulset/postgres --timeout=240s
> kubectl -n atrium rollout status deployment/redis --timeout=180s
> kubectl -n atrium rollout status deployment/rabbitmq --timeout=300s
> kubectl -n atrium create configmap migrations --from-file=migrations --dry-run=client -o yaml | kubectl apply -f -
> kubectl -n atrium delete job migrate --ignore-not-found
> kubectl apply -f deploy/k8s/20-migrate-job.yaml
> kubectl -n atrium wait --for=condition=complete job/migrate --timeout=240s
> kubectl apply -f deploy/k8s/30-api.yaml -f deploy/k8s/31-ingest.yaml -f deploy/k8s/32-processor.yaml
> kubectl -n atrium rollout status deployment/api --timeout=180s
> kubectl -n atrium rollout status deployment/ingest --timeout=180s
> kubectl -n atrium rollout status deployment/processor --timeout=180s
> kubectl -n atrium get pods

k8s-seed:       ## register the 100 simulated devices
> kubectl -n atrium exec -i postgres-0 -- sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"' < seeds/devices.sql

k8s-sim:        ## start the simulator (after seeding)
> kubectl apply -f deploy/k8s/40-simulator.yaml
> kubectl -n atrium rollout status deployment/simulator --timeout=120s

k8s-status:
> kubectl -n atrium get pods,svc

k8s-stats:
> kubectl -n atrium exec postgres-0 -- sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "SELECT (SELECT count(*) FROM readings) AS readings, (SELECT count(*) FROM alerts) AS alerts;"'
> kubectl -n atrium exec deploy/rabbitmq -- rabbitmqctl list_queues name messages consumers

k8s-forward:    ## API on http://localhost:18080 (Ctrl+C to stop)
> kubectl -n atrium port-forward svc/api 18080:8080

k8s-down:       ## delete the kind cluster
> kind delete cluster --name atrium

.PHONY: grafana grafana-stop

grafana:        ## Grafana dashboard on http://localhost:3000
> docker compose --profile dashboard up -d grafana
> @echo "Dashboard: http://localhost:3000"

grafana-stop:
> docker compose --profile dashboard stop grafana
