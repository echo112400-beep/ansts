.PHONY: all lint build test ci start docker-up docker-down

all: ci

lint:
	node scripts/ci_check.mjs --stage lint

build:
	node scripts/ci_check.mjs --stage build

test:
	node scripts/ci_check.mjs --stage test

ci:
	node scripts/ci_check.mjs --stage all

start:
	bash ./scripts/run-local.sh

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down
