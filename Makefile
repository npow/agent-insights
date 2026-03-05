.DEFAULT_GOAL := help

VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

.PHONY: install dev test clean help setup ingest digest reset

## Install from source (uses agenttrace from GitHub)
install:
	@echo "Installing agent-insights..."
	pip install -e .
	@echo "Done! Run with: agent-insights"

## Install using a local agenttrace checkout (for development)
install-dev:
	@echo "Installing agent-insights with local agenttrace..."
	pip install -e ../agenttrace/packages/agenttrace -e .
	@echo "Done!"

## Run in development mode (browser)
dev:
	python -m agent_insights

## Setup launchd services (macOS)
setup:
	python -m agent_insights setup

## Run full pipeline (ingest + judge)
ingest:
	python -m agent_insights ingest

## Generate weekly digest
digest:
	python -m agent_insights digest

## Reset database
reset:
	python -m agent_insights reset

## Run tests
test:
	pytest tests/ -v

## Clean build artifacts
clean:
	rm -rf dist/ build/
	rm -rf agent_insights.egg-info claude_retro.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

## Install watchdog for file watching
install-watchdog:
	pip install watchdog

## Show this help
help:
	@echo "Agent Insights - Build targets:"
	@echo ""
	@awk '/^##/ { \
		desc = substr($$0, 4); \
		getline; \
		if (match($$1, /^[a-zA-Z_-]+:/)) { \
			target = substr($$1, 1, length($$1)-1); \
			printf "  \033[36m%-20s\033[0m %s\n", target, desc \
		} \
	}' $(MAKEFILE_LIST)
	@echo ""
	@echo "Environment:"
	@echo "  Version:    $(VERSION)"
	@echo "  Commit:     $(COMMIT)"
	@echo "  Build Date: $(BUILD_DATE)"
