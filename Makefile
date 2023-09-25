default: docker-compose-up

all:

docker-image:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	docker compose -f rabbitmq-compose.yaml up -d --build
	sleep 5
	docker compose -f docker-compose.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f rabbitmq-compose.yaml stop -t 1
	docker compose -f rabbitmq-compose.yaml down --remove-orphans
	docker compose -f docker-compose.yaml down --remove-orphans
.PHONY: docker-compose-down