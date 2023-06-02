.PHONY: test

run:
	docker-compose up --build -d mongo1 mongo2 mongo3 nats1 nats2 nats3 connector

stop:
	docker-compose down -v --remove-orphans

test:
	go test -v -cover ./...

it:
	docker-compose up --build --abort-on-container-exit --exit-code-from connector-it