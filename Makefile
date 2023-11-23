default: docker-compose-up

all:

# docker-image:
# 	docker build -f ./server/Dockerfile -t "server:latest" .
# 	docker build -f ./client/Dockerfile -t "client:latest" .
# 	docker build -f ./column_cleaner/Dockerfile -t "column_cleaner:latest" .
# 	docker build -f ./filter_by_three_stopovers/Dockerfile -t "filter_by_three_stopovers:latest" .
# 	docker build -f ./query_handler/Dockerfile -t "query_handler:latest" .
# 	docker build -f ./result_handler/Dockerfile -t "result_handler:latest" .
# 	docker build -f ./avg_calculator/Dockerfile -t "avg_calculator:latest" .
# 	docker build -f ./filter_by_average/Dockerfile -t "filter_by_average:latest" .
# 	docker build -f ./group_by/Dockerfile -t "group_by:latest" .
# 	docker build -f ./reducer_group_by/Dockerfile -t "reducer_group_by:latest" .
# 	docker build -f ./distance_calculator/Dockerfile -t "distance_calculator:latest" .
# .PHONY: docker-image

docker-image:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./column_cleaner/Dockerfile -t "column_cleaner:latest" .
	docker build -f ./filter_by_three_stopovers/Dockerfile -t "filter_by_three_stopovers:latest" .
	docker build -f ./query_handler/Dockerfile -t "query_handler:latest" .
	docker build -f ./result_handler/Dockerfile -t "result_handler:latest" .
	docker build -f ./group_by/Dockerfile -t "group_by:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	docker compose -f docker-compose.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose.yaml stop -t 20
	docker compose -f docker-compose.yaml down --remove-orphans
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs

docker-compose-scaled-up: docker-image
	sudo rm -f column_cleaner/*.txt
	sudo rm -f result_handler/*.txt
	sudo rm -f filter_by_three_stopovers/*.txt
	docker compose -f docker-compose-scaled.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-scaled-down:
	docker compose -f docker-compose-scaled.yaml stop -t 20
	docker compose -f docker-compose-scaled.yaml down --remove-orphans
.PHONY: docker-compose-down

docker-compose-scaled-logs:
	docker compose -f docker-compose-scaled.yaml logs -f
.PHONY: docker-compose-logs