default: docker-compose-up

all:

docker-image:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./initial_column_cleaner/Dockerfile -t "initial_column_cleaner:latest" .
#	docker build -f ./filter_by_three_stopovers/Dockerfile -t "filter_by_three_stopovers:latest" .
#   docker build -f ./query_2_column_filter/Dockerfile -t "query_2_column_filter:latest" .
	docker build -f ./query_handler/Dockerfile -t "query_handler:latest" .
	docker build -f ./avg_calculator/Dockerfile -t "avg_calculator:latest" .
	docker build -f ./filter_by_average/Dockerfile -t "filter_by_average:latest" .
	docker build -f ./group_by/Dockerfile -t "group_by:latest" .
	docker build -f ./reducer_group_by/Dockerfile -t "reducer_group_by:latest" .
	docker build -f ./fastests_calculator/Dockerfile -t "fastests_calculator:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	docker compose -f rabbitmq-compose.yaml up -d --build
	sleep 30
	docker compose -f docker-compose.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f rabbitmq-compose.yaml stop -t 1
	docker compose -f rabbitmq-compose.yaml down --remove-orphans
	docker compose -f docker-compose.yaml down --remove-orphans
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs