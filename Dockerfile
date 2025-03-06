# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS build
WORKDIR /go/src/github.com/damianiandrea/mongodb-nats-connector
COPY go.* ./
RUN go mod download
COPY . .
RUN go build -o /go/bin/connector ./cmd/connector

FROM alpine:3
WORKDIR /root/
COPY --from=build /go/bin/connector ./
CMD ./connector