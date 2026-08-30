.PHONY: build test lint clean up down ps

BIN_DIR := bin
EDGE_BIN := $(BIN_DIR)/pharos-edge
INGESTION_BIN := $(BIN_DIR)/pharos-ingestion

build:
	go build -buildvcs=false -o $(EDGE_BIN) ./cmd/pharos-edge
	go build -buildvcs=false -o $(INGESTION_BIN) ./cmd/pharos-ingestion

test:
	go test -buildvcs=false -v -race ./...

lint:
	go vet -buildvcs=false ./...

clean:
	rm -f $(EDGE_BIN) $(INGESTION_BIN) *.db *.db-wal *.db-shm *.db-journal coverage.out

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps
