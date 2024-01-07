.PHONY: test

test:
	go test -v -cover ./...

run:
	docker-compose up --build -d mongo1 mongo2 mongo3 nats1 nats2 nats3 connector

run-mongo-nats:
	docker-compose up --build -d mongo1 mongo2 mongo3 nats1 nats2 nats3

create-connector:
	docker-compose up --build --no-start connector

run-it:
	docker-compose up --build connector-it

stop:
	docker-compose down -v --remove-orphans

it: run-mongo-nats create-connector run-it stop