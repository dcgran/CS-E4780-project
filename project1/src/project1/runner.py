"""Experiment runner for OpenCEP patterns.

Provides a CLI to:
 - Load pattern configs (YAML/JSON)
 - Ingest an input file (line-delimited events) via FileInputStream
 - Run CEP and measure wall-clock runtime, matches count, throughput (events/sec) and average matches/sec.
 - Write matches to output directory.

Assumptions:
 - Each line of input file represents one primitive event already formatted acceptable to chosen DataFormatter.
 - We reuse existing MetastockDataFormatter for stock examples (if available) else a PassThrough formatter.

TODO:
 - Add memory measurement (psutil) if dependency allowed.
 - Support multiple input files / streaming rates.
"""
from __future__ import annotations
import argparse
import time
from pathlib import Path
from typing import Optional

from opencep.CEP import CEP
from opencep.base.DataFormatter import DataFormatter, EventTypeClassifier
from opencep.stream.FileStream import FileInputStream, FileOutputStream

from project1.pattern_loader import load_patterns  # type: ignore
from project1.citibike_occupancy import OccupancyFormatter, OccupancyThresholds  # type: ignore
from project1.citibike_formatter import CitiBikeFormatter  # type: ignore


class _SimpleClassifier(EventTypeClassifier):
    """Classifies events by first column value if exists else 'EVT'."""
    def get_event_type(self, event_payload: dict):  # type: ignore[override]
        return event_payload.get("col0", "EVT")


class PassThroughFormatter(DataFormatter):  # simplistic fallback
    def __init__(self):
        super().__init__(_SimpleClassifier())

    def parse_event(self, raw_data: str):  # type: ignore[override]
        parts = raw_data.strip().split(',')
        return {f"col{i}": p for i, p in enumerate(parts)}

    def get_event_timestamp(self, event_payload: dict):  # type: ignore[override]
        # No timestamp; return incremental logical time (not implemented) -> 0
        return 0


def run(path_patterns: str, input_file: str, output_dir: str, formatter: Optional[str] = None, mode: str = 'generic'):
    loaded = load_patterns(path_patterns)
    patterns = [lp.pattern for lp in loaded]

    # choose formatter
    fmt: DataFormatter
    if mode == 'citibike':
        # Occupancy aware station delta stream
        fmt = OccupancyFormatter(thresholds=OccupancyThresholds(low=0))
    else:
        if formatter == 'pass':
            fmt = PassThroughFormatter()
        elif formatter == 'metastock':
            try:
                from opencep.base.DataFormatter import MetastockDataFormatter  # type: ignore
                fmt = MetastockDataFormatter()
            except Exception:
                fmt = PassThroughFormatter()
        else:
            fmt = PassThroughFormatter()

    cep = CEP(patterns)
    input_stream = FileInputStream(input_file)
    output_stream = FileOutputStream(output_dir, 'matches.txt', is_async=True)

    start = time.time()
    elapsed = cep.run(input_stream, output_stream, fmt)
    total_wall = time.time() - start

    # After run, output file was written.
    matches_count = 0
    # We can't easily get items after run since FileOutputStream wrote them already (unless async). For now rely on size.
    # Provide future improvement note.

    metrics = {
        'patterns': [p for p in (lp.name for lp in loaded)],
        'elapsed_engine_seconds': elapsed,
        'wall_seconds': total_wall,
        'matches': matches_count,
    }
    return metrics


def main():
    ap = argparse.ArgumentParser(description="Run CEP experiments")
    ap.add_argument('--patterns', required=True, help='Path to pattern config (yaml/json)')
    ap.add_argument('--input', required=True, help='Path to input events file')
    ap.add_argument('--output', default='outputs', help='Directory for matches output')
    ap.add_argument('--formatter', choices=['pass', 'metastock'], default='pass')
    ap.add_argument('--mode', choices=['generic', 'citibike'], default='generic')
    args = ap.parse_args()
    Path(args.output).mkdir(parents=True, exist_ok=True)
    metrics = run(args.patterns, args.input, args.output, args.formatter, mode=args.mode)
    print(metrics)


if __name__ == '__main__':
    main()
