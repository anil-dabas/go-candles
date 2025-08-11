proto:
	protoc --go_out=. --go-grpc_out=. internal/proto/candles.proto

test:
	go test ./... -cover

build:
	go build ./cmd/candles
	go build ./cmd/client

PHONY: all
# generate all
all:
	make proto;
	make build;