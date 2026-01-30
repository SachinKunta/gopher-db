.PHONY: build test lint clean run bench

# Build
build:
	go build -o bin/gopher-db ./cmd/server

# Run
run: build
	./bin/gopher-db

# Test
test:
	go test -v -race ./...

# Test with coverage
cover:
	go test -race -coverprofile=coverage.txt -covermode=atomic ./...
	go tool cover -html=coverage.txt -o coverage.html

# Lint
lint:
	golangci-lint run ./...

# Benchmark
bench:
	go test -bench=. -benchmem ./...

# Clean
clean:
	rm -rf bin/
	rm -f coverage.txt coverage.html
	rm -f *.prof
