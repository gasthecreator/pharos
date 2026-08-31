.PHONY: build test lint fmt fmt-check clean up down ps

BIN_DIR := bin
EDGE_BIN := $(BIN_DIR)/pharos-edge
INGESTION_BIN := $(BIN_DIR)/pharos-ingestion
CONSUMER_BIN := $(BIN_DIR)/pharos-consumer
CLI_BIN := $(BIN_DIR)/pharos-cli

build:
	go build -buildvcs=false -o $(EDGE_BIN) ./cmd/pharos-edge
	go build -buildvcs=false -o $(INGESTION_BIN) ./cmd/pharos-ingestion
	go build -buildvcs=false -o $(CONSUMER_BIN) ./cmd/pharos-consumer
	go build -buildvcs=false -o $(CLI_BIN) ./cmd/pharos-cli

test:
	go test -buildvcs=false -v -race ./...

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
	rm -f $(EDGE_BIN) $(INGESTION_BIN) $(CONSUMER_BIN) $(CLI_BIN) *.db *.db-wal *.db-shm *.db-journal coverage.out

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps
