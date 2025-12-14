up: down
	@docker-compose -f=docker-compose.yaml -p spark up -d

down:
	@docker-compose -p spark down