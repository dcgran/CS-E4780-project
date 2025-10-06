# CS-E4780 Project 1: Efficient Pattern Detection over Data Streams

High-performance Complex Event Processing (CEP) system with pattern-aware load shedding and real-time adaptive streaming for detecting bike trip chains in CitiBike data.

## Quick Start

### Data Setup

**Download 2018 CitiBike NYC Data**:

1. Visit the CitiBike trip data archive: [https://s3.amazonaws.com/tripdata/index.html](https://s3.amazonaws.com/tripdata/index.html)
2. Download the following 2018 files (or use the direct links below):
   - `2018-citibike-tripdata.zip`
     - Containing `201801-citibike-tripdata.csv.zip` through `201812-citibike-tripdata.csv.zip`
3. Extract the CSV files to the `data/` directory
4. Run the sorting script to ensure chronological order:
   ```bash
   # Sort all data files by timestamp (required for time window matching)
   for file in data/2018*.csv; do
       tmpfile="${file}.tmp"
       head -1 "$file" > "$tmpfile"
       tail -n +2 "$file" | sort -t',' -k2,2 >> "$tmpfile"
       mv "$tmpfile" "$file"
   done
   ```

**Direct Download Links** (placeholder - update with actual storage):

```bash
# Example: Download from project storage or cloud bucket
wget https://your-storage-url/201801-citibike-tripdata.csv -P data/
# ... repeat for other months
```

> **Note**: The data files in this repository have already been **sorted by timestamp**. If you download fresh files from CitiBike, make sure to run the sorting script above, as CEP time window matching requires chronological order.

### Running the System

```bash
# Install dependencies
uv sync

# Run on 2018 dataset
uv run cep-runner --input data/201801-citibike-tripdata.csv --verbose

# Test with limited events
uv run cep-runner --input data/201801-citibike-tripdata.csv --max-lines 10000 --verbose

# Get JSON output for analysis
uv run cep-runner --input data/201801-citibike-tripdata.csv --json
```
