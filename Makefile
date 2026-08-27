.PHONY: up down test clean

COMPOSE_FILE := docker/docker-compose.yml

up:
	docker compose -f $(COMPOSE_FILE) up -d --wait
	./gradlew bootJar
	./scripts/start-services.sh

down:
	./scripts/stop-services.sh
	docker compose -f $(COMPOSE_FILE) down -v

test:
	./gradlew test

clean:
	./gradlew clean
