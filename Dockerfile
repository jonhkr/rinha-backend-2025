FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . ./

RUN go build -o /main

# Final image
FROM alpine:latest
COPY --from=builder /main /main

EXPOSE 8080

CMD ["/main"]