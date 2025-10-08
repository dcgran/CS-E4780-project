# CS-E4780 Project 1: Efficient Pattern Detection over Data Streams

High-performance Complex Event Processing (CEP) system with pattern-aware load shedding and real-time adaptive streaming for detecting bike trip chains in CitiBike data.

## Quick Start

### Data Setup

**Download 2018 CitiBike NYC Data**:

**Option 1: Automated Download (Recommended)**

```bash
# Complete setup with automatic download
make setup
```

**Option 2: Manual Download**

1. Visit the CitiBike trip data archive: [https://s3.amazonaws.com/tripdata/index.html](https://s3.amazonaws.com/tripdata/index.html)
2. Download `2018-citibike-tripdata.zip` manually, or use the commands below
3. Extract the CSV files to the `data/` directory
4. Sort the data files by timestamp:
   ```bash
   make sort-data
   ```

**Direct Download Link** (CitiBike S3 Bucket):

```bash
# Download and extract 2018 NYC data
cd data/
curl -sL -o 2018-citibike-tripdata.zip https://s3.amazonaws.com/tripdata/2018-citibike-tripdata.zip
unzip 2018-citibike-tripdata.zip
rm 2018-citibike-tripdata.zip
cd ..
```

> **Note**: The data files in this repository have already been **sorted by timestamp**. If you download fresh files from CitiBike, make sure to run the sorting script above, as CEP time window matching requires chronological order.

### Running the System

```bash
# First-time setup (install dependencies + sort data)
make setup

# Quick demo with 1,000 events
make run-demo

# Run on full dataset
make run-full

# Generate benchmark results (10k events)
make benchmark

# Show all available commands
make help
```

**Manual Commands** (alternative to make targets):

```bash
# Install dependencies manually
uv sync

# Run specific commands
uv run cep-runner --input data/201801-citibike-tripdata.csv --verbose
uv run cep-runner --input data/201801-citibike-tripdata.csv --max-lines 10000 --json
```

## Makefile Automation

The project includes a lean Makefile for easy task automation:

| Command                   | Description                                     |
| ------------------------- | ----------------------------------------------- |
| `make help`               | Show all available commands                     |
| `make setup`              | Complete setup (install + download + sort data) |
| `make install`            | Install dependencies with uv                    |
| `make download-data`      | Download 2018 NYC CitiBike data from S3 bucket  |
| `make sort-data`          | Sort CSV files by timestamp (required for CEP)  |
| `make run-demo`           | Quick demo with 1,000 events                    |
| `make run-full`           | Run CEP on full dataset                         |
| `make benchmark`          | Generate JSON benchmark results (10k events)    |
| `make benchmark-baseline` | Run baseline (no load shedding) for evaluation  |
| `make evaluate`           | Run complete performance evaluation             |
| `make clean`              | Clean output files and caches                   |
| `make info`               | Show dataset information                        |
| `make check-data`         | Check if CSV data files exist                   |

**First-time users**: Run `make setup` to install dependencies and prepare data files.

## Development

### Testing Commands

```bash
# Quick validation (1k events)
make run-demo

# Performance benchmarking (10k events)
make benchmark

# Show dataset information
make info

# Check if data files exist
make check-data
```

**Manual Commands** (if not using Makefile):

```bash
uv run cep-runner --input data/201801-citibike-tripdata.csv --max-lines 1000 --verbose
uv run cep-runner --input data/201801-citibike-tripdata.csv --max-lines 10000 --json
```

### Project Structure

```
├── src/project1/
│   ├── __init__.py                        # Package marker
│   ├── cep_runner.py                      # Main streaming runner
│   ├── hot_paths_patterns.py              # Pattern definitions (2018 NYC stations)
│   └── citibike_formatter.py              # Event formatter (handles 2017/2018)
├── packages/opencep/                      # OpenCEP framework (local)
├── data/
│   ├── 201801-citibike-tripdata.csv      # January 2018 (sorted)
│   ├── 201802-citibike-tripdata.csv      # February 2018 (sorted)
│   └── ... (201803-201812)               # March-December 2018 (sorted)
├── outputs/
│   ├── matches.txt                        # Pattern matches
│   └── full_dataset_results.json          # Performance metrics
├── pyproject.toml                         # Project config with cep-runner script
├── BENCHMARK_RESULTS.md                   # Detailed benchmark report
└── README.md                              # This file
```

**Script Alias**: The project includes a `cep-runner` script alias defined in `pyproject.toml`:

```toml
[project.scripts]
cep-runner = "project1.cep_runner:main"
```

Use `uv run cep-runner` instead of `uv run python src/project1/cep_runner.py`.

### Generate Results for Report

```bash
# Generate benchmark results (10k events)
make benchmark

# View dataset information
make info
```

**Manual Commands** (if not using Makefile):

```bash
uv run cep-runner --input data/201801-citibike-tripdata.csv --max-lines 10000 --json > results.json
uv run cep-runner --input data/201801-citibike-tripdata.csv --max-lines 1000 --json
uv run cep-runner --input data/201801-citibike-tripdata.csv --max-lines 5000 --json
uv run cep-runner --input data/201801-citibike-tripdata.csv --max-lines 10000 --json
```
