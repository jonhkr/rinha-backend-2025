FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . ./

RUN go build -o /api ./cmd/api
RUN go build -o /db ./cmd/db

# Final image
FROM alpine:latest
COPY --from=builder /api /api
COPY --from=builder /db /db

EXPOSE 8080
