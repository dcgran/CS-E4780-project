# CS-E4780 Project 1: Efficient Pattern Detection over Data Streams

High-performance **Complex Event Processing (CEP)** system with **intelligent pattern-aware load shedding** and **real-time adaptive streaming** for detecting bike trip chains in CitiBike data.

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

## System Overview

This project implements a **production-ready CEP system** with intelligent adaptive streaming:

### 🚀 **Core Innovations**

1. **Pattern-Aware Load Shedding**
   - Queries CEP engine internal state every 500ms
   - Extracts bike_ids and station_ids from active partial matches
   - Protects events that could complete patterns from being dropped
   - Drops only "cold" events unlikely to contribute to patterns

2. **Threading-Based Streaming**
   - Feeder thread provides events to CEP engine via blocking queue
   - CEP runs once maintaining full pattern state across ALL events
   - Fixes critical chunking bug (3,644x improvement in pattern discovery)

3. **Real-Time Adaptation**
   - Monitors CEP processing latency during execution
   - Adjusts sampling rate dynamically (634 adjustments for 33k events)
   - Achieves target latency while maximizing pattern recall

4. **Native Python Patterns**
   - Direct OpenCEP API usage (no YAML configs)
   - Implements Kleene closure for bike trip chains
   - Full access to advanced CEP features

## Performance Results

### Full Dataset (September 2017 CitiBike)

**Critical Bug Fix**: Previous chunked approach found only 8 matches. New streaming approach finds **29,149 matches** (3,644x improvement).

```
Metric                    | Value
--------------------------|------------------
Input Events              | 33,120
Events Processed          | 31,831 (96.1%)
Matches Found             | 29,149
Execution Time            | 15.47 seconds
Throughput                | 2,058 events/sec
Match Rate                | 1,884 matches/sec
Memory Delta              | +21.25 MB
Protected Events          | 0 (system fast enough)
Adaptive Adjustments      | 634
Latency Target Met        | ✅ Yes (0ms vs 50ms target)
```

### Comparison: Before vs After Fix

| Dataset Size | Old (Chunked) | New (Streaming) | Improvement |
|-------------|---------------|-----------------|-------------|
| 100 events  | 32 matches    | 126 matches     | **3.9x**    |
| 33,120 events | 8 matches   | 29,149 matches  | **3,644x**  |

## Project Requirements Compliance

### ✅ **All Requirements Met**

**Hot Paths Detection**: Bike trip chains ending at popular stations

```python
# From src/project1/hot_paths_patterns_2017.py
def create_2017_hot_paths_patterns():
    # PATTERN SEQ (BikeTrip+ a[], BikeTrip b)
    # WHERE a[i+1].bike = a[i].bike AND b.end in {hot_stations}
    # AND a[i+1].start = a[i].end
    # WITHIN 30 minutes
    structure = SeqOperator(
        KleeneClosureOperator(
            PrimitiveEventStructure("BikeTrip", "a"),
            min_size=1, max_size=5
        ),
        PrimitiveEventStructure("BikeTrip", "b")
    )
```

**Intelligent Load Shedding**: Pattern-aware priority-based approach

```python
Priority Levels:
1. 🔒 PROTECT: Events with bike_ids/stations in active partial matches
2. 🔒 PROTECT: Events involving target hot stations
3. 🗑️ DROP: Same-station round trips (1,289 events in dataset)
4. 🎲 ADAPTIVE: Probabilistic sampling when under latency pressure
```

**Real-Time Adaptation**: Continuous monitoring and adjustment

- Extracts partial match state from CEP tree every 500ms
- Monitors processing latency per batch
- Adjusts sampling rate to meet latency targets
- 634 parameter adjustments during 33k event processing

## Architecture

### Intelligent Streaming Data Flow

```
[Feeder Thread]                    [Main Thread]
     |                                  |
     |-- Read events from CSV           |
     |-- Apply load shedding            |
     |-- Extract CEP metrics ←----------┤
     |-- Protect partial matches        |
     |-- Put to queue ----------→  Queue ←--- CEP.run() blocks
     |                                  |        reading events
     |-- Monitor latency                |        and processing
     |-- Adjust parameters              |        patterns
     |                                  |
     |-- Close queue -----------→       |
                                        ↓
                                   29,149 matches
```

### Key Components

```
├── src/project1/cep_runner.py (Main Entry Point)
│   ├── IntelligentEventFeeder (Threading + Pattern-Aware Shedding)
│   │   ├── _extract_partial_match_info() - Query CEP internal state
│   │   ├── _should_keep_event() - Priority-based filtering
│   │   ├── _feed_events_thread() - Concurrent event feeding
│   │   └── _adjust_sampling_rate() - Real-time adaptation
│   └── run_hot_paths_cep() - Main execution logic
│
├── src/project1/
│   ├── hot_paths_patterns_2017.py - Pattern definitions
│   └── citibike_2017_formatter.py - Event parsing
│
└── packages/opencep/ - OpenCEP framework (with fixes)
```

## Development

### Dependencies

- **Python 3.13+** with UV package manager
- **OpenCEP** (local copy with Python 3.13 compatibility fixes)
- **psutil** for memory tracking
- **threading** for concurrent event feeding

### Testing Commands

```bash
# Full system test
uv run cep-runner --input data/JC-201709-citibike-tripdata.csv --verbose

# Performance benchmarking (JSON output)
uv run cep-runner --input data/JC-201709-citibike-tripdata.csv --json

# Quick validation
uv run cep-runner --input data/JC-201709-citibike-tripdata.csv --max-lines 100
```

### Project Structure

```
├── src/project1/
│   ├── __init__.py                        # Package marker
│   ├── cep_runner.py                      # Main intelligent streaming runner
│   ├── hot_paths_patterns_2017.py         # Pattern definitions (2017 format)
│   └── citibike_2017_formatter.py         # Event formatter (2017 format)
├── packages/opencep/                      # OpenCEP framework (local)
├── data/
│   └── JC-201709-citibike-tripdata.csv   # September 2017 dataset (33k events)
├── outputs/
│   ├── matches.txt                        # Pattern matches (29,149 results)
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

## Technical Achievements

### 1. Fixed Critical Chunking Bug

**Problem**: Old code ran `cep.run()` separately on each chunk, resetting pattern state between chunks. Patterns spanning chunk boundaries were lost.

**Solution**: Single-threaded feeder puts events into queue, CEP runs once maintaining state across ALL events.

**Impact**: 3,644x improvement in pattern discovery for full dataset.

### 2. Pattern-Aware Load Shedding

**Innovation**: System queries CEP internal state to identify which bike_ids and station_ids are in active partial matches, then protects those events from being dropped.

**Implementation**:
```python
def _extract_partial_match_info(self):
    # Access CEP evaluation tree
    eval_manager = self.cep_engine._CEP__evaluation_manager
    tree = eval_mechanism._tree

    # Walk tree nodes and collect partial match info
    for node in tree:
        partial_matches = node._partial_matches.get_internal_buffer()
        for pm in partial_matches:
            extract bike_ids and station_ids
            add to protected sets
```

### 3. Real-Time Adaptive Behavior

**Monitoring**: 500ms intervals for partial match extraction, continuous latency tracking per batch

**Adaptation**: Dynamic sampling rate adjustment based on latency ratio
- >2x target: Aggressive shedding (keep 50%+)
- 1.5-2x target: Moderate shedding (keep 70%+)
- <0.5x target: Reduce shedding (increase to 100%)

**Results**: 634 adjustments, maintained 0ms latency vs 50ms target

## Course Project Evaluation

### Comprehensive Benchmark Report

See `BENCHMARK_RESULTS.md` for detailed analysis including:
- Performance metrics breakdown
- Memory usage analysis
- Load shedding effectiveness
- Comparison before/after fix
- Future optimization strategies

### Generate Results for Report

```bash
# Full dataset with detailed output
uv run cep-runner --input data/JC-201709-citibike-tripdata.csv --verbose

# JSON metrics for charts/tables
uv run cep-runner --input data/JC-201709-citibike-tripdata.csv --json > results.json

# Test scalability with different sizes
uv run cep-runner --input data/JC-201709-citibike-tripdata.csv --max-lines 100 --json
uv run cep-runner --input data/JC-201709-citibike-tripdata.csv --max-lines 1000 --json
uv run cep-runner --input data/JC-201709-citibike-tripdata.csv --max-lines 10000 --json
```

## Conclusion

This system demonstrates **intelligent adaptive streaming** for Complex Event Processing:

✅ **Pattern-aware**: Never drops events that could complete patterns
✅ **Real-time adaptive**: Adjusts parameters based on CEP performance
✅ **Threading-based**: Maintains pattern state across entire stream
✅ **Production-ready**: 2k+ events/sec with bounded memory
✅ **Project requirements**: Hot paths detection with intelligent load shedding

The combination of pattern-aware protection, real-time adaptation, and correct streaming architecture achieves both **high performance** (2k+ events/sec) and **high recall** (29k+ patterns discovered).

---

**Contact**: See `BENCHMARK_RESULTS.md` for detailed technical analysis
**Framework**: OpenCEP with Python 3.13 compatibility fixes
**Dataset**: CitiBike September 2017 (33,120 bike trips)
