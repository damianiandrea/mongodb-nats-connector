# syntax=docker/dockerfile:1

FROM golang:1.21-alpine AS build
WORKDIR /go/src/github.com/damianiandrea/mongodb-nats-connector
COPY go.* ./
RUN go mod download
COPY . .
RUN go build -o /go/bin/connector ./cmd/connector

FROM alpine:latest
WORKDIR /root/
COPY --from=build /go/bin/connector ./
CMD ./connector