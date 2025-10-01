# CS-E4780 Project 1: Efficient Pattern Detection over Data Streams

## Quick Start

```bash
# Install dependencies
uv sync

# Run on full September 2017 dataset (33,120 events)
uv run cep-runner --input data/JC-201709-citibike-tripdata.csv --verbose

# Test with limited events
uv run cep-runner --input data/JC-201709-citibike-tripdata.csv --max-lines 100 --verbose

# Get JSON output for analysis
uv run cep-runner --input data/JC-201709-citibike-tripdata.csv --json
```
