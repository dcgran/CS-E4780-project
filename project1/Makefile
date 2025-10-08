# CS-E4780 Project 1 - CEP System Makefile

.PHONY: help install sort-data run-demo run-full benchmark clean check-data info setup download-data benchmark-baseline evaluate

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies with uv
	@echo "Installing dependencies with uv..."
	uv sync

check-data: ## Check if CSV data files exist
	@echo "Checking for data files in data/ directory..."
	@if [ ! -d "data" ]; then \
		echo "❌ data/ directory not found"; \
		exit 1; \
	fi
	@files=$$(ls data/*.csv 2>/dev/null | wc -l); \
	if [ $$files -eq 0 ]; then \
		echo "❌ No CSV files found in data/ directory"; \
		echo "Please download CitiBike 2018 data files first"; \
		echo "See README.md for download instructions"; \
		exit 1; \
	else \
		echo "✅ Found $$files CSV file(s) in data/ directory"; \
	fi

sort-data: check-data ## Sort all CSV files by timestamp (required for CEP)
	@echo "Sorting CSV files by timestamp..."
	@for file in data/*.csv; do \
		if [ -f "$$file" ]; then \
			echo "Sorting $$file..."; \
			tmpfile="$${file}.tmp"; \
			head -1 "$$file" > "$$tmpfile"; \
			tail -n +2 "$$file" | sort -t',' -k2,2 >> "$$tmpfile"; \
			mv "$$tmpfile" "$$file"; \
			echo "✅ Sorted $$file"; \
		fi; \
	done
	@echo "✅ All CSV files sorted by timestamp"

run-demo: check-data ## Quick demo with 1,000 events
	@echo "Running CEP demo with 1,000 events..."
	uv run cep-runner --input data/201801-citibike-tripdata.csv --max-lines 1000 --verbose

run-full: check-data ## Run CEP on full dataset
	@echo "Running CEP on full dataset..."
	@echo "Processing data/201801-citibike-tripdata.csv (full dataset)..."
	uv run cep-runner --input data/201801-citibike-tripdata.csv --verbose

benchmark: check-data ## Generate JSON benchmark results (10k events)
	@echo "Generating benchmark results..."
	@mkdir -p outputs
	@echo "Benchmarking with data/201801-citibike-tripdata.csv (10k events)..."
	uv run cep-runner --input data/201801-citibike-tripdata.csv --max-lines 10000 --json > outputs/benchmark_results.json
	@echo "✅ Benchmark results saved to outputs/benchmark_results.json"

clean: ## Clean output files and caches
	@echo "Cleaning output files..."
	rm -f outputs/*.txt outputs/*.json
	rm -rf src/project1/__pycache__/
	rm -rf packages/opencep/opencep/__pycache__/
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ Cleaned output files and caches"

info: check-data ## Show dataset information
	@echo "Dataset Information:"
	@echo "==================="
	@for file in data/*.csv; do \
		if [ -f "$$file" ]; then \
			lines=$$(wc -l < "$$file"); \
			size=$$(ls -lh "$$file" | awk '{print $$5}'); \
			echo "$$file: $$lines lines, $$size"; \
		fi; \
	done

download-data: ## Download 2018 NYC CitiBike data from S3 bucket
	@mkdir -p data
	@if [ -f "data/201801-citibike-tripdata.csv" ]; then \
		echo "✅ Data already exists: data/201801-citibike-tripdata.csv"; \
	else \
		echo "Downloading 2018 NYC CitiBike data from S3..."; \
		cd data && \
		echo "Downloading 2018-citibike-tripdata.zip..." && \
		curl -sL -o 2018-citibike-tripdata.zip https://s3.amazonaws.com/tripdata/2018-citibike-tripdata.zip && \
		echo "Extracting files..." && \
		unzip -q 2018-citibike-tripdata.zip && \
		echo "Moving CSV files to data directory..." && \
		mv 2018-citibike-tripdata/*.csv . 2>/dev/null || true && \
		echo "Cleaning up..." && \
		rm -rf 2018-citibike-tripdata __MACOSX 2018-citibike-tripdata.zip && \
		echo "✅ Downloaded and extracted 2018 NYC CitiBike data"; \
	fi

benchmark-baseline: check-data ## Run baseline (no load shedding) for evaluation
	@echo "Running baseline benchmark (no load shedding)..."
	@mkdir -p outputs
	uv run cep-runner --input data/201801-citibike-tripdata.csv --max-lines 10000 --json --no-load-shedding > outputs/baseline.json
	@echo "✅ Baseline results saved to outputs/baseline.json"

evaluate: check-data ## Run performance evaluation (baseline + bounded runs)
	@echo "Running performance evaluation..."
	uv run cep-runner --evaluate --input data/201801-citibike-tripdata.csv --max-lines 10000

setup: install download-data sort-data ## Complete setup for new users (install + download + sort data)
	@echo "✅ Setup complete! Try 'make run-demo' to test the system."
