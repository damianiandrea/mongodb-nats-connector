MONGO_VERSION ?= 6.0-focal

.PHONY: test

test:
	go test -v -race -cover ./...

create-env:
	echo MONGO_VERSION=$(MONGO_VERSION) > .env

run-mongo-nats: create-env
	docker-compose up --build -d mongo1 mongo2 mongo3 nats1 nats2 nats3

run: run-mongo-nats
	docker-compose up --build connector

create-connector:
	docker-compose up --build --no-start connector

run-it:
	docker-compose up --build connector-it

stop:
	docker-compose down -v --remove-orphans

it: run-mongo-nats create-connector run-it stop