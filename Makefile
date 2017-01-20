build:
	go build -o ./bin/kafka-mysql ./cmd/kafka-mysql/main.go

test:
	go test ./...