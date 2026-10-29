.PHONY: all lint build test integration-test ci

all: ci

lint:
	NODE_PATH=/usr/share/nodejs node scripts/ci_check.mjs --stage lint

build:
	NODE_PATH=/usr/share/nodejs node scripts/ci_check.mjs --stage build

test:
	NODE_PATH=/usr/share/nodejs node scripts/ci_check.mjs --stage test

integration-test: test

ci:
	NODE_PATH=/usr/share/nodejs node scripts/ci_check.mjs --stage all
