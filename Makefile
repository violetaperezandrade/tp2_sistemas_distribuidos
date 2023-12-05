default: docker-compose-up

all:

docker-image:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./column_cleaner/Dockerfile -t "column_cleaner:latest" .
	docker build -f ./filter_by_three_stopovers/Dockerfile -t "filter_by_three_stopovers:latest" .
	docker build -f ./query_handler/Dockerfile -t "query_handler:latest" .
	docker build -f ./result_handler/Dockerfile -t "result_handler:latest" .
	docker build -f ./group_by/Dockerfile -t "group_by:latest" .
	docker build -f ./reducer_group_by/Dockerfile -t "reducer_group_by:latest" .
	docker build -f ./distance_calculator/Dockerfile -t "distance_calculator:latest" .
	docker build -f ./avg_calculator/Dockerfile -t "avg_calculator:latest" .
	docker build -f ./filter_by_average/Dockerfile -t "filter_by_average:latest" .
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

docker-compose-scaled-up: docker-image docker-compose-scaled-delete
	docker compose -f docker-compose-scaled.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-scaled-down:
	docker compose -f docker-compose-scaled.yaml stop -t 20
	docker compose -f docker-compose-scaled.yaml down --remove-orphans
.PHONY: docker-compose-down

docker-compose-scaled-logs:
	docker compose -f docker-compose-scaled.yaml logs -f
.PHONY: docker-compose-logs

docker-compose-scaled-delete:
	sudo rm -r ./distance_calculator/distance_calculator_1 || true
	sudo rm -r ./distance_calculator/distance_calculator_2 || true
	sudo rm -r ./distance_calculator/distance_calculator_3 || true
	sudo rm -r ./group_by/group_by_airport || true
	sudo rm -r ./group_by/group_by_route || true
	sudo rm -f ./group_by/*.txt
	sudo rm -r results || true
	sudo rm -f column_cleaner/*.txt
	sudo rm -f result_handler/*.txt
	sudo rm -r ./reducer_group_by/3 || true
	sudo rm -r ./reducer_group_by/5 || true
	sudo rm -r ./reducer_group_by/4 || true
	sudo rm -r ./avg_calculator/avg_calculator_1 || true
	sudo rm -r ./avg_calculator/avg_calculator_2 || true
	sudo rm -r ./avg_calculator/avg_calculator_3 || true
	sudo rm -r ./filter_by_average/filter_by_average_1 || true
	sudo rm -r ./filter_by_average/filter_by_average_2 || true
	sudo rm -r ./filter_by_average/filter_by_average_3 || true
	sudo rm -r ./group_by/group_by_route_query_4 || true
	sudo rm -r ./filter_by_three_stopovers/filter_by_three_stopovers_1 || true
	sudo rm -r ./filter_by_three_stopovers/filter_by_three_stopovers_2 || true
	sudo rm -r ./filter_by_three_stopovers/filter_by_three_stopovers_3 || true
.PHONY: docker-compose-scaled-delete