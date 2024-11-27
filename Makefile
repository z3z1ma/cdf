.PHONY: all test lint format scan scan-new-baseline scan-without-baseline

all: format lint test

lint:
	@uvx ruff check

format:
	@uvx ruff check --fix --select I
	@uvx ruff format --preview

test:
	@uv run pytest

scan:
	@uvx bandit -r src -b tests/bandit_baseline.json

scan-new-baseline:
	@uvx bandit -r src -f json -o tests/bandit_baseline.json

scan-without-baseline:
	@uvx bandit -r src
