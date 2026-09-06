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
test:
	go test -buildvcs=false -v -race -count=1 -p 1 $$(go list ./... | grep -v '/internal/chaos$$')
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

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps
