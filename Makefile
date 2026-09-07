.PHONY: build test lint fmt fmt-check clean up down ps

BIN_DIR := bin
EDGE_BIN := $(BIN_DIR)/pharos-edge
INGESTION_BIN := $(BIN_DIR)/pharos-ingestion
CONSUMER_BIN := $(BIN_DIR)/pharos-consumer
CLI_BIN := $(BIN_DIR)/pharos-cli
DASHBOARD_BIN := $(BIN_DIR)/pharos-dashboard

build:
	go build -buildvcs=false -o $(EDGE_BIN) ./cmd/pharos-edge
	go build -buildvcs=false -o $(INGESTION_BIN) ./cmd/pharos-ingestion
	go build -buildvcs=false -o $(CONSUMER_BIN) ./cmd/pharos-consumer
	go build -buildvcs=false -o $(CLI_BIN) ./cmd/pharos-cli
	go build -buildvcs=false -o $(DASHBOARD_BIN) ./cmd/pharos-dashboard

# -p 1 (serialized packages) and running internal/chaos separately/last
# both matter, not just style -- see .github/workflows/ci.yml's own test
# steps for the full reasoning (concurrent packages' real Cassandra/Kafka
# connections genuinely OOM-killed this project's dev topology once TLS was
# added; internal/chaos's disruptive tests running immediately before
# internal/consumer's in alphabetical order caused intermittent spurious
# failures there too). `go test ./...` without both of these is not just
# slower, it can flake or OOM a real cluster -- this target exists so
# CONTRIBUTING.md's own "before opening a PR" instructions actually match
# what CI safely does, not a shortcut CI itself doesn't take.
#
# RAPID_CHECKS raises pgregory.net/rapid's default 100 generated cases per
# property test (Slice 22) -- without it, `make test` silently exercises
# 50x fewer property-test cases than CI actually does, despite
# CONTRIBUTING.md's own claim that CI runs the same checks as `make test`.
test:
	RAPID_CHECKS=5000 go test -buildvcs=false -v -race -count=1 -p 1 $$(go list ./... | grep -v '/internal/chaos$$')
	go test -buildvcs=false -v -race -count=1 ./internal/chaos/...

lint:
	go vet -buildvcs=false ./...

fmt:
	gofmt -w .

fmt-check:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "Not gofmt-formatted:"; echo "$$unformatted"; exit 1; \
	fi

clean:
	rm -f $(EDGE_BIN) $(INGESTION_BIN) $(CONSUMER_BIN) $(CLI_BIN) $(DASHBOARD_BIN) *.db *.db-wal *.db-shm *.db-journal coverage.out

# Bringing every container up at once with a bare `docker compose up -d`
# starts MirrorMaker 2 before any Kafka topic exists, which makes it
# busy-loop discovery/retry and has caused real OOM kills on this host
# (docker-compose.yml's own Kafka section comment, ARCHITECTURE_PROPOSALS.md's
# Slice 14 addendum #3, and .github/workflows/ci.yml's identical sequencing).
# This target mirrors CI's/scripts/demo.sh's safe order instead: certs,
# then Cassandra+Kafka+Redis+observability, then wait for healthy, then
# topics, then MirrorMaker 2 last.
up:
	@if [ ! -f certs/ca-cert.pem ]; then \
		./scripts/generate_certs.sh; \
	else \
		echo "certs/ already exists, skipping generation."; \
		echo "Regenerating breaks TLS trust for already-running containers --"; \
		echo "run 'docker compose down && ./scripts/generate_certs.sh && make up'"; \
		echo "if you need fresh certs."; \
	fi
	docker compose up -d cassandra-1 cassandra-2 cassandra-3 cassandra-4 kafka-1 kafka-2 kafka-3 kafka-4 prometheus grafana redis
	@echo "Waiting for Cassandra + Kafka + Redis to report healthy..."
	@for i in $$(seq 1 90); do \
		healthy=true; \
		for c in pharos-cassandra-1 pharos-cassandra-2 pharos-cassandra-3 pharos-cassandra-4 pharos-kafka-1 pharos-kafka-2 pharos-kafka-3 pharos-kafka-4 pharos-redis; do \
			status=$$(docker inspect --format '{{.State.Health.Status}}' "$$c" 2>/dev/null || echo missing); \
			if [ "$$status" != "healthy" ]; then healthy=false; fi; \
		done; \
		if [ "$$healthy" = true ]; then echo "Cluster healthy."; break; fi; \
		sleep 3; \
	done
	./scripts/create_topics.sh
	docker compose up -d mirrormaker

down:
	docker compose down

ps:
	docker compose ps
