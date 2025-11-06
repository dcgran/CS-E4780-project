# CS-E4780 Project 1 - CEP System Makefile

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies with uv
	@echo "Installing dependencies with uv..."
	uv sync

run-graph-rag: ## Run graph rag
	uv run marimo run graph_rag.py

benchmark-baseline: ## Run baseline benchmark (no KNN, no validation)
	uv run python benchmark.py baseline

benchmark-knn: ## Run KNN benchmark (with KNN few-shot and validation)
	uv run python benchmark.py knn
