.PHONY: all test lint format scan scan-new-baseline scan-without-baseline

all: format lint test

lint:
	@uv tool run ruff check

format:
	@uv tool run ruff check --fix --select I
	@uv tool run ruff format --preview

test:
	@uv run pytest

scan:
	@uv tool run bandit -r src -b tests/bandit_baseline.json

scan-new-baseline:
	@uv tool run bandit -r src -f json -o tests/bandit_baseline.json

scan-without-baseline:
	@uv tool run bandit -r src
