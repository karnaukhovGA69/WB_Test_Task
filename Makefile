APP_NAME := search-trends
CMD_PATH := ./cmd/app
PROTO_DIR := proto
PROTO_FILES := $(shell find $(PROTO_DIR) -name "*.proto")

.PHONY: proto build run test bench docker-up docker-down lint

proto:
	go run github.com/bufbuild/buf/cmd/buf@v1.45.0 generate

build:
	go build -o bin/$(APP_NAME) $(CMD_PATH)

run:
	CONFIG_PATH=config.example.yaml go run $(CMD_PATH)

test:
	go test ./...

bench:
	go test -bench=. -benchmem ./...

docker-up:
	docker compose up --build

docker-down:
	docker compose down -v

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		go vet ./...; \
	fi
