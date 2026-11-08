.PHONY: all lint build test ci clean

all: ci

lint:
	node scripts/ci_check.mjs --stage lint

build:
	node scripts/ci_check.mjs --stage build

test:
	node scripts/ci_check.mjs --stage test

ci:
	node scripts/ci_check.mjs --stage all

clean:
	rm -rf dist
