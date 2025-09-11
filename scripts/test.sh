#!/bin/bash
set -e

echo "Running Go tests..."

# Run all tests with coverage
go test -v -race -coverprofile=coverage.out ./...

# Generate coverage report
if [ -f coverage.out ]; then
    go tool cover -html=coverage.out -o coverage.html
    echo "Coverage report generated: coverage.html"
    go tool cover -func=coverage.out | grep total:
fi

# Run linter
echo "Running linter..."
golangci-lint run

echo "All tests passed!"