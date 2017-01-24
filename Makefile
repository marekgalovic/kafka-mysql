build:
	go get -t -d -v ./...
	go build -o ./bin/kafka-mysql ./cmd/kafka-mysql/main.go

test:
	script/setup-mysql
	go test ./...