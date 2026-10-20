.PHONY: check lint format typecheck test all

all: check  ## Run all checks (default)

check: lint format typecheck test  ## Run lint, format, typecheck, and tests

lint:  ## Run ruff linter
	uv run ruff check .

format:  ## Check formatting (no changes)
	uv run ruff format --check .

typecheck:  ## Run mypy type checker
	uv run mypy src

test:  ## Run test suite
	uv run pytest tests/ -q

fix:  ## Auto-fix lint and format issues
	uv run ruff check --fix .
	uv run ruff format .
