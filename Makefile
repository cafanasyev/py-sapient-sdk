.PHONY: sync test test-cov lint lint-fix format format-check typecheck check lock clean

sync:
	uv sync

test:
	uv run pytest

test-cov:
	uv run pytest --cov=sapient_sdk --cov-report=xml --cov-report=term

lint:
	uv run ruff check .

lint-fix:
	uv run ruff check --fix .

format:
	uv run ruff format .

format-check:
	uv run ruff format --check .

typecheck:
	uv run mypy

check: lint format-check typecheck test

lock:
	uv lock

clean:
	rm -rf .mypy_cache .ruff_cache .pytest_cache
	find . -type d -name __pycache__ -not -path "./.venv/*" -exec rm -rf {} +