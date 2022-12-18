# syntax=docker/dockerfile:1

FROM golang:1.19-alpine AS build
WORKDIR /go/src/github.com/damianiandrea/go-mongo-nats-connector
COPY go.* ./
RUN go mod download
COPY . .
RUN go build -o /go/bin/connector ./cmd/connector

ARG CONFIG_FILE=connector.yaml
FROM alpine:latest
WORKDIR /root/
COPY --from=build /go/bin/connector ./
ARG CONFIG_FILE
COPY ${CONFIG_FILE} ./
CMD ./connector