# Stage 1: Build the Go application binary
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Copy dependency files and download
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the application source code
COPY . .

# Build the server binary statically
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main ./server/main.go

# Stage 2: Create a minimal deployment container
FROM alpine:latest  
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the pre-built binary from the builder stage
COPY --from=builder /app/main .

# Expose ports:
# - 8080: Web interface and SSE Gateway
# - 50051: gRPC server
EXPOSE 8080
EXPOSE 50051

# Run the binary
CMD ["./main"]
