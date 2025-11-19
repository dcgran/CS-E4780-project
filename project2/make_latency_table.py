"""
Usage:
uv run make_latency_table.py --output latency.csv
"""

from __future__ import annotations
import argparse
import json
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List

RESULTS_FILE = "benchmark_suite_results.json"


def load_suite(path: str | Path) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Results file not found: {p}")
    with p.open() as f:
        return json.load(f)


def ms(x: float) -> float:
    return round(x * 1000, 1)


def safe_mean(values: List[float]) -> float:
    return ms(mean(values)) if values else 0.0


def classify_stage_times(
    results: List[Dict[str, Any]],
) -> Dict[str, Dict[str, List[float]]]:
    """Return per-stage timing buckets with two classes: without_cache, with_cache_hit."""
    buckets = {
        "schema_pruning": {"without_cache": [], "with_cache_hit": []},
        "text2cypher": {"without_cache": [], "with_cache_hit": []},
        "query_execution": {"without_cache": [], "with_cache_hit": []},
        "answer_generation": {"without_cache": [], "with_cache_hit": []},
    }

    for r in results:
        t = r.get("timing_breakdown", {})
        # Skip malformed rows (e.g., errors)
        if not all(
            k in t
            for k in [
                "schema_pruning",
                "query_generation",
                "query_execution",
                "answer_generation",
            ]
        ):
            continue

        sp = t["schema_pruning"]
        tg = t["query_generation"]
        qe = t["query_execution"]
        ag = t["answer_generation"]

        prune_hit = bool(t.get("cache_hit_prune"))
        query_hit = bool(t.get("cache_hit_query"))

        # Direct classification from flags
        (
            buckets["schema_pruning"]["with_cache_hit"]
            if prune_hit
            else buckets["schema_pruning"]["without_cache"]
        ).append(sp)
        (
            buckets["text2cypher"]["with_cache_hit"]
            if query_hit
            else buckets["text2cypher"]["without_cache"]
        ).append(tg)

        # Derived classification for execution and answer: use only pure cold or pure hit rows
        if not prune_hit and not query_hit:
            buckets["query_execution"]["without_cache"].append(qe)
            buckets["answer_generation"]["without_cache"].append(ag)
        elif prune_hit and query_hit:
            buckets["query_execution"]["with_cache_hit"].append(qe)
            buckets["answer_generation"]["with_cache_hit"].append(ag)
        # Mixed rows are ignored for these stages to keep the table simple

    return buckets


def summarize_buckets(
    buckets: Dict[str, Dict[str, List[float]]],
) -> Dict[str, Dict[str, float]]:
    summary: Dict[str, Dict[str, float]] = {}
    for stage, per_bucket in buckets.items():
        summary[stage] = {b: safe_mean(vals) for b, vals in per_bucket.items()}
    return summary


def compute_total(summary: Dict[str, Dict[str, float]]) -> Dict[str, float]:
    return {
        "without_cache": sum(
            summary[stage].get("without_cache", 0.0) for stage in summary
        ),
        "with_cache_hit": sum(
            summary[stage].get("with_cache_hit", 0.0) for stage in summary
        ),
    }


def process_config_csv(
    config_name: str, results: List[Dict[str, Any]]
) -> List[List[str]]:
    """Return CSV rows for one configuration (two columns only)."""
    buckets = classify_stage_times(results)
    summary = summarize_buckets(buckets)  # ms values already
    total = compute_total(summary)
    rows: List[List[str]] = []
    # Per stage rows
    for stage_key, stage_name in [
        ("schema_pruning", "Schema Pruning"),
        ("text2cypher", "Text2Cypher"),
        ("query_execution", "Query Execution"),
        ("answer_generation", "Response Generation"),
    ]:
        s = summary[stage_key]
        rows.append(
            [
                config_name,
                stage_name,
                f"{s.get('without_cache', 0.0)}",
                f"{s.get('with_cache_hit', 0.0)}",
            ]
        )
    # Total row
    rows.append(
        [
            config_name,
            "Total",
            f"{total['without_cache']}",
            f"{total['with_cache_hit']}",
        ]
    )
    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Generate latency breakdown LaTeX rows."
    )
    parser.add_argument(
        "--file", default=RESULTS_FILE, help="Path to benchmark_suite_results.json"
    )
    parser.add_argument("--config", help="Single configuration name to process")
    parser.add_argument("--output", help="Write LaTeX output to file instead of stdout")
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available configuration names and exit",
    )
    args = parser.parse_args()

    data = load_suite(args.file)

    if args.list:
        print("Available configurations:")
        for name in data.keys():
            print(f"  - {name}")
        return

    configs = [args.config] if args.config else list(data.keys())
    csv_rows: List[List[str]] = [
        ["Config", "Stage", "WithoutCache_ms", "WithCacheHit_ms"]
    ]
    for name in configs:
        if name not in data:
            print(f"Warning: config '{name}' not found; skipping.")
            continue
        results = data[name].get("results", [])
        csv_rows.extend(process_config_csv(name, results))

    # Serialize
    output_text = "\n".join([",".join(r) for r in csv_rows]) + "\n"
    if args.output:
        Path(args.output).write_text(output_text)
        print(f"CSV latency breakdown written to {args.output}")
    else:
        print(output_text)


if __name__ == "__main__":
    main()
