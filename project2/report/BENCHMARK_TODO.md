# Benchmark TODO - Ultra-Compressed Version

This document lists all placeholders to fill after running benchmarks.

## Section 5: Performance Evaluation

### Methodology (Line 299)
- `[X]` - Total events in dataset
- `[Y]` - Days covered
- `[Z]` - Average events/minute

### Table 1: Performance Evaluation Results (Lines 314-320)

Run these commands and fill in the table:

```bash
# Baseline (no load shedding)
uv run cep-runner --input data/201801-citibike-tripdata.csv --no-load-shedding --json > baseline.json

# 10% latency bound
uv run cep-runner --input data/201801-citibike-tripdata.csv --latency-bound 0.1 --json > lb10.json

# 30% latency bound
uv run cep-runner --input data/201801-citibike-tripdata.csv --latency-bound 0.3 --json > lb30.json

# 50% latency bound
uv run cep-runner --input data/201801-citibike-tripdata.csv --latency-bound 0.5 --json > lb50.json

# 70% latency bound
uv run cep-runner --input data/201801-citibike-tripdata.csv --latency-bound 0.7 --json > lb70.json

# 90% latency bound
uv run cep-runner --input data/201801-citibike-tripdata.csv --latency-bound 0.9 --json > lb90.json
```

**Table Placeholders:**

| Row      | Events Proc | Matches | Recall | Latency | Throughput | Memory |
|----------|-------------|---------|--------|---------|------------|--------|
| Baseline | `[X]`       | `[M]`   | 100    | `[L]`   | `[Z]`      | `[P]`  |
| 10% LB   | `[X1]`      | `[M1]`  | `[R1]` | `[L1]`  | `[TP1]`    | `[P1]` |
| 30% LB   | `[X2]`      | `[M2]`  | `[R2]` | `[L2]`  | `[TP2]`    | `[P2]` |
| 50% LB   | `[X3]`      | `[M3]`  | `[R3]` | `[L3]`  | `[TP3]`    | `[P3]` |
| 70% LB   | `[X4]`      | `[M4]`  | `[R4]` | `[L4]`  | `[TP4]`    | `[P4]` |
| 90% LB   | `[X5]`      | `[M5]`  | `[R5]` | `[L5]`  | `[TP5]`    | `[P5]` |

**Calculate Recall:**
- `[R1]` = (M1 / M) × 100
- `[R2]` = (M2 / M) × 100
- etc.

### Prose Placeholders (Lines 324-329)

**Recall vs. Latency Trade-off (Line 325):**
- `[D1]` - Drop rate at 10% bound (percentage)
- `[R1]` - Recall at 10% bound
- Replace the entire bracketed section with actual analysis

**Scalability (Line 327):**
- `[X]` - Events processed (from baseline)
- `[Y]` - Execution time seconds (from baseline)
- `[Z]` - Throughput events/sec (from baseline)
- `[P]` - Peak memory MB (from baseline)
- `[60×Z]` - Calculate: Z × 60 for events/min capacity
- `[ZZ]` - Dataset average events/min (from methodology line 299)
- `[G]` - Capacity gap multiplier: (Z × 60) / ZZ

**Optimization Impact (Line 329):**
- `[Z]` - Throughput from baseline
- `[M]` - Matches found from baseline

### Limitations (Lines 333-335)

- `[ZZ]` - Dataset events/min (same as line 299)
- `[G]` - Capacity gap (same calculation as line 327)
- Replace `[If applicable: ...]` with actual observation about 0ms latency

---

## Quick Reference

**Priority order:**
1. **Baseline** - provides `[X]`, `[Y]`, `[Z]`, `[M]`, `[P]`, `[L]`
2. **Latency bounds** - provides all other values

**Calculations needed:**
- Recall: `(Matches / Baseline_Matches) × 100`
- Capacity gap: `(Baseline_Throughput × 60) / Dataset_EventsPerMin`
- Drop rate: `100 - (Events_Processed / Total_Events) × 100`

**Dataset info:**
- Verify January 2018 data exists at: `data/201801-citibike-tripdata.csv`
- If not, adjust path in commands above

---

## Notes

- Run ALL benchmarks on the **same machine** for consistency
- Record hardware specs for limitations section
- Save JSON outputs for easy parsing
- Total benchmarks: 6 runs (1 baseline + 5 latency bounds)
- Expected runtime: [depends on dataset size]

---

## After Filling Placeholders

1. Remove all `%TODO` comments
2. Compile LaTeX to verify formatting
3. Check table alignment and readability
4. Ensure all cross-references resolve
5. Verify calculations (recall, capacity gap) are correct
