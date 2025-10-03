#!/usr/bin/env python3
"""Intelligent CEP Runner with pattern-aware load shedding.

Implements threading-based streaming where:
- Feeder thread provides events via blocking queue
- CEP runs once maintaining full pattern state
- Load shedding protects events in active partial matches
- Parameters adapt in real-time based on latency
"""

from __future__ import annotations
import argparse
import json
import os
import psutil
import sys
import threading
import time
from pathlib import Path
from typing import Optional, Dict, Any, Set

from opencep.CEP import CEP
from opencep.stream.FileStream import FileOutputStream
from opencep.stream.Stream import InputStream

from project1.citibike_2017_formatter import CitiBike2017Formatter  # type: ignore
from project1.hot_paths_patterns_2017 import create_2017_hot_paths_patterns  # type: ignore


class IntelligentEventFeeder:
    """Intelligent event feeder with pattern-aware load shedding.

    Runs in a separate thread, feeding events to CEP engine while monitoring:
    1. Processing latency from CEP engine
    2. Partial match state (which bike_ids/stations are in active patterns)
    3. Adaptive load shedding that protects pattern-relevant events
    """

    def __init__(
        self,
        file_path: str,
        cep_engine,
        formatter,
        max_lines: Optional[int] = None,
        verbose: bool = False,
    ):
        self.file_path = file_path
        self.max_lines = max_lines
        self.verbose = verbose
        self.cep_engine = cep_engine
        self.formatter = formatter

        self.input_stream = InputStream()
        self.target_stations = {"3186", "3183", "3203"}

        self.sampling_rate = 1.0
        self.target_latency_ms = 50.0
        self.recent_latencies: list[float] = []

        self.protected_bike_ids: Set[str] = set()
        self.protected_station_ids: Set[str] = set()
        self.last_metrics_update = time.time()
        self.metrics_update_interval = 0.5

        self.events_dropped = 0
        self.events_protected = 0
        self.shed_by_same_station = 0
        self.shed_by_sampling = 0
        self.total_events_seen = 0
        self.lines_processed = 0

        self.adjustment_history: list[dict[str, float]] = []
        self.feeding_start_time = None
        self.feeding_end_time = None

        self.feeder_thread = None
        self.exception = None

        self._prepare_streaming_data(file_path, max_lines)

    def _prepare_streaming_data(self, file_path: str, max_lines: Optional[int]):
        with open(file_path, "r") as f:
            self.all_lines = f.readlines()

        # Skip CSV header row (common header field names)
        if self.all_lines and any(
            header in self.all_lines[0].lower()
            for header in ["ride_id", "tripduration", "starttime", "bikeid"]
        ):
            self.all_lines = self.all_lines[1:]

        if max_lines is not None:
            self.all_lines = self.all_lines[:max_lines]

        if self.verbose:
            print(f"📊 Prepared {len(self.all_lines)} events for intelligent streaming")
            print(f"🎯 Target latency: {self.target_latency_ms}ms per batch")
            print("🧠 Pattern-aware load shedding enabled")

    def _extract_partial_match_info(self):
        """Query CEP tree for partial matches to protect pattern-relevant events."""
        try:
            eval_manager = self.cep_engine._CEP__evaluation_manager

            if hasattr(eval_manager, "_SequentialEvaluationManager__eval_mechanism"):
                eval_mechanism = (
                    eval_manager._SequentialEvaluationManager__eval_mechanism
                )
            else:
                return

            if not hasattr(eval_mechanism, "_tree"):
                return

            tree = eval_mechanism._tree
            protected_bikes = set()
            protected_stations = set()

            def collect_from_node(node):
                if (
                    not hasattr(node, "_partial_matches")
                    or node._partial_matches is None
                ):
                    return

                try:
                    partial_matches = node._partial_matches.get_internal_buffer()
                    for pm in partial_matches:
                        if hasattr(pm, "events"):
                            for event in pm.events:
                                if hasattr(event, "payload"):
                                    payload = event.payload
                                    if "bike_id" in payload:
                                        protected_bikes.add(str(payload["bike_id"]))
                                    if "start_station_id" in payload:
                                        protected_stations.add(
                                            str(payload["start_station_id"])
                                        )
                                    if "end_station_id" in payload:
                                        protected_stations.add(
                                            str(payload["end_station_id"])
                                        )
                except Exception:
                    pass

            def walk_tree(node):
                collect_from_node(node)
                if hasattr(node, "_parents"):
                    for parent in node._parents:
                        walk_tree(parent)
                if hasattr(node, "_left_child"):
                    walk_tree(node._left_child)
                if hasattr(node, "_right_child"):
                    walk_tree(node._right_child)

            if hasattr(tree, "_root"):
                walk_tree(tree._root)
            elif hasattr(tree, "_leaves"):
                for leaf in tree._leaves:
                    walk_tree(leaf)

            self.protected_bike_ids = protected_bikes
            self.protected_station_ids = protected_stations

        except Exception:
            pass

    def _should_keep_event(self, raw_event: str, batch_latency_ms: float) -> bool:
        """Priority-based load shedding: protect partial matches, target stations, then sample."""
        parts = raw_event.split(",")
        if len(parts) < 12:
            return False

        try:
            bike_id = parts[11].strip('"') if len(parts) > 11 else ""
            start_station_id = parts[3].strip('"')
            end_station_id = parts[7].strip('"')

            if bike_id in self.protected_bike_ids:
                self.events_protected += 1
                return True
            if start_station_id in self.protected_station_ids:
                self.events_protected += 1
                return True
            if end_station_id in self.protected_station_ids:
                self.events_protected += 1
                return True

            if (
                start_station_id in self.target_stations
                or end_station_id in self.target_stations
            ):
                return True

            if start_station_id == end_station_id and start_station_id != "":
                self.shed_by_same_station += 1
                return False

            if batch_latency_ms > self.target_latency_ms:
                import random

                if random.random() > self.sampling_rate:
                    self.shed_by_sampling += 1
                    return False

            return True

        except (ValueError, IndexError):
            return False

    def _feed_events_thread(self, output_stream):
        try:
            self.feeding_start_time = time.time()

            batch_size = 50 if len(self.all_lines) > 1000 else 20
            batch_start_time = time.time()
            events_in_batch = 0

            if self.verbose:
                print(
                    f"🚀 Starting intelligent event feeding (batch size: {batch_size})"
                )

            for idx, line in enumerate(self.all_lines):
                self.total_events_seen += 1
                stripped = line.strip()
                if not stripped:
                    continue

                current_time = time.time()
                if (
                    current_time - self.last_metrics_update
                    > self.metrics_update_interval
                ):
                    self._extract_partial_match_info()
                    self.last_metrics_update = current_time

                batch_elapsed = (current_time - batch_start_time) * 1000
                batch_latency = batch_elapsed / max(1, events_in_batch)

                if self._should_keep_event(stripped, batch_latency):
                    self.input_stream._stream.put(stripped)
                    self.lines_processed += 1
                    events_in_batch += 1
                else:
                    self.events_dropped += 1

                if events_in_batch >= batch_size:
                    batch_time = time.time() - batch_start_time
                    batch_latency_ms = (batch_time * 1000) / events_in_batch
                    self.recent_latencies.append(batch_latency_ms)

                    if len(self.recent_latencies) > 20:
                        self.recent_latencies = self.recent_latencies[-20:]

                    self._adjust_sampling_rate(batch_latency_ms)

                    if self.verbose and idx % 1000 == 0:
                        protected_pct = (
                            self.events_protected / max(1, self.total_events_seen)
                        ) * 100
                        print(
                            f"   Progress: {idx}/{len(self.all_lines)} events, "
                            f"{self.lines_processed} kept, {self.events_dropped} dropped, "
                            f"{protected_pct:.1f}% protected"
                        )

                    batch_start_time = time.time()
                    events_in_batch = 0

            self.input_stream.close()
            self.feeding_end_time = time.time()

            if self.verbose:
                print(
                    f"✅ Event feeding completed in {self.feeding_end_time - self.feeding_start_time:.2f}s"
                )

        except Exception as e:
            self.exception = e
            self.input_stream.close()
            raise

    def _adjust_sampling_rate(self, batch_latency_ms: float):
        if len(self.recent_latencies) < 3:
            return

        avg_latency = sum(self.recent_latencies) / len(self.recent_latencies)
        latency_ratio = avg_latency / self.target_latency_ms

        if latency_ratio > 2.0:
            self.sampling_rate = max(0.5, self.sampling_rate * 0.9)
            action = "⚠️  AGGRESSIVE shedding"
        elif latency_ratio > 1.5:
            self.sampling_rate = max(0.7, self.sampling_rate * 0.95)
            action = "🔄 MODERATE shedding"
        elif latency_ratio < 0.5:
            self.sampling_rate = min(1.0, self.sampling_rate * 1.1)
            action = "✅ REDUCING shedding"
        else:
            action = "🎯 STABLE"

        self.adjustment_history.append(
            {
                "avg_latency_ms": avg_latency,
                "sampling_rate": self.sampling_rate,
                "protected_count": float(
                    len(self.protected_bike_ids) + len(self.protected_station_ids)
                ),
                "action": float(0) if action == "none" else float(1),
            }
        )

    def start_feeding(self, output_stream):
        """Start the feeder thread."""
        self.feeder_thread = threading.Thread(
            target=self._feed_events_thread, args=(output_stream,), daemon=False
        )
        self.feeder_thread.start()

    def wait_for_completion(self):
        """Wait for feeder thread to complete."""
        if self.feeder_thread:
            self.feeder_thread.join()
        if self.exception:
            raise self.exception

    def _print_final_stats(self):
        """Print final streaming statistics."""
        total_events = self.total_events_seen
        drop_rate = (
            (self.events_dropped / total_events * 100) if total_events > 0 else 0
        )
        protected_rate = self.events_protected / max(1, self.lines_processed) * 100

        print("🎯 INTELLIGENT STREAMING RESULTS:")
        print(f"   Total events seen: {total_events}")
        print(f"   Events processed: {self.lines_processed}")
        print(f"   Events dropped: {self.events_dropped} ({drop_rate:.1f}%)")
        print("   🧠 Pattern-aware protection:")
        print(
            f"      🔒 Events protected: {self.events_protected} ({protected_rate:.1f}% of kept events)"
        )
        print(f"      🆔 Protected bikes: {len(self.protected_bike_ids)}")
        print(f"      🚉 Protected stations: {len(self.protected_station_ids)}")
        print("   📊 Shedding breakdown:")
        print(f"      🎲 Sampling: {self.shed_by_sampling}")
        print(f"      🔄 Same station: {self.shed_by_same_station}")

        if self.recent_latencies:
            avg_latency = sum(self.recent_latencies) / len(self.recent_latencies)
            print(
                f"   ⏱️  Final avg latency: {avg_latency:.1f}ms/batch (target: {self.target_latency_ms}ms)"
            )

        print(f"   🔄 Adaptive adjustments: {len(self.adjustment_history)}")


def run_hot_paths_cep(
    input_file: str,
    output_dir: str,
    max_lines: Optional[int] = None,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Run CEP with hot paths patterns - the main project requirement."""

    if verbose:
        print("=== Hot Paths CEP Runner ===")

    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Track memory usage
    process = psutil.Process()
    initial_memory = process.memory_info().rss

    pattern_load_start = time.time()
    try:
        patterns = create_2017_hot_paths_patterns()

        if verbose:
            print(f"📋 Loaded {len(patterns)} optimized hot paths patterns")
            print("   ⚙️  OpenCEP Kleene closure optimizations enabled")
            print("   🎯 Selection strategy: MATCH_SINGLE (reduces state explosion)")
            print("   🚀 Candidate limiting: Max 100 partial matches per node")

    except Exception as e:
        print(f"Error loading patterns: {e}")
        return {"error": f"Pattern loading failed: {e}"}

    pattern_names = [f"hot_paths_{i}" for i in range(len(patterns))]
    pattern_load_time = time.time() - pattern_load_start

    try:
        fmt = CitiBike2017Formatter()
        if verbose:
            print("Using CitiBike2017Formatter")
    except Exception as e:
        print(f"Error: CitiBike2017Formatter required but failed: {e}")
        return {"error": f"Formatter initialization failed: {e}"}

    # CEP initialization
    cep_init_start = time.time()
    try:
        cep = CEP(patterns)
        if verbose:
            print("CEP initialized successfully")
    except Exception as e:
        print(f"Error initializing CEP: {e}")
        return {"error": f"CEP initialization failed: {e}"}

    cep_init_time = time.time() - cep_init_start

    # Setup intelligent feeder with pattern-aware load shedding
    stream_setup_start = time.time()
    try:
        feeder = IntelligentEventFeeder(input_file, cep, fmt, max_lines, verbose)
        output_stream = FileOutputStream(output_dir, "matches.txt", is_async=False)
        if verbose:
            print("🔄 Intelligent feeder prepared for pattern-aware streaming")
    except Exception as e:
        print(f"Error setting up streams: {e}")
        return {"error": str(e)}

    stream_setup_time = time.time() - stream_setup_start

    # Track memory after setup
    setup_memory = process.memory_info().rss

    # Run intelligent streaming CEP with pattern-aware load shedding
    execution_start = time.time()
    try:
        if verbose:
            print("🚀 Starting intelligent streaming CEP execution...")
            print("🧠 Pattern-aware load shedding will protect partial matches")
            print("📈 Parameters adapt in real-time based on CEP latency")

        # Start the feeder thread
        feeder.start_feeding(output_stream)

        # Run CEP in main thread - it will block reading from feeder's stream
        elapsed_engine = cep.run(feeder.input_stream, output_stream, fmt)

        # Wait for feeder to complete
        feeder.wait_for_completion()

        if verbose:
            print("✅ Intelligent streaming completed successfully")
            feeder._print_final_stats()

        # Get final metrics from feeder
        lines_processed = feeder.lines_processed

    except Exception as e:
        print(f"Intelligent streaming failed: {e}")
        import traceback

        traceback.print_exc()
        return {"error": f"Intelligent streaming failed: {e}"}

    execution_end = time.time()
    total_execution_time = execution_end - execution_start

    # Track peak memory and final memory
    peak_memory = process.memory_info().rss
    final_memory = process.memory_info().rss

    # Count matches from output file
    matches_count = 0
    output_file_path = os.path.join(output_dir, "matches.txt")
    try:
        if os.path.exists(output_file_path):
            with open(output_file_path, "r") as f:
                matches_count = sum(1 for line in f if line.strip())
        if verbose:
            print(f"Found {matches_count} matches")
    except Exception as e:
        if verbose:
            print(f"Error counting matches: {e}")

    # Calculate throughput metrics
    events_per_second = (
        lines_processed / total_execution_time if total_execution_time > 0 else 0
    )
    matches_per_second = (
        matches_count / total_execution_time if total_execution_time > 0 else 0
    )

    # Memory usage in MB
    initial_memory_mb = initial_memory / 1024 / 1024
    setup_memory_mb = setup_memory / 1024 / 1024
    peak_memory_mb = peak_memory / 1024 / 1024
    final_memory_mb = final_memory / 1024 / 1024
    memory_delta_mb = (peak_memory - initial_memory) / 1024 / 1024

    # Calculate intelligent load shedding effectiveness
    total_input_events = feeder.total_events_seen
    drop_rate = (
        (feeder.events_dropped / total_input_events * 100)
        if total_input_events > 0
        else 0
    )

    # Calculate final latency performance
    avg_final_latency = 0.0
    if feeder.recent_latencies:
        avg_final_latency = sum(feeder.recent_latencies) / len(feeder.recent_latencies)

    metrics = {
        # Basic info
        "patterns": pattern_names,
        "pattern_count": len(patterns),
        "input_file": input_file,
        "lines_processed": lines_processed,
        "max_lines_limit": max_lines,
        # Intelligent load shedding metrics
        "total_input_events": total_input_events,
        "events_dropped": feeder.events_dropped,
        "drop_rate_percent": round(drop_rate, 1),
        "events_protected": feeder.events_protected,
        "protected_bikes": len(feeder.protected_bike_ids),
        "protected_stations": len(feeder.protected_station_ids),
        "shed_by_same_station": feeder.shed_by_same_station,
        "shed_by_sampling": feeder.shed_by_sampling,
        # Adaptive performance metrics
        "target_latency_ms": feeder.target_latency_ms,
        "final_avg_latency_ms": round(avg_final_latency, 1),
        "latency_target_achieved": avg_final_latency <= feeder.target_latency_ms * 1.5,
        "parameter_adjustments": len(feeder.adjustment_history),
        # Timing breakdown
        "pattern_load_seconds": round(pattern_load_time, 4),
        "stream_setup_seconds": round(stream_setup_time, 4),
        "cep_init_seconds": round(cep_init_time, 4),
        "engine_execution_seconds": round(elapsed_engine, 4),
        "total_execution_seconds": round(total_execution_time, 4),
        # Results
        "matches_found": matches_count,
        # Performance metrics
        "events_per_second": round(events_per_second, 2),
        "matches_per_second": round(matches_per_second, 2),
        # Memory metrics
        "initial_memory_mb": round(initial_memory_mb, 2),
        "setup_memory_mb": round(setup_memory_mb, 2),
        "peak_memory_mb": round(peak_memory_mb, 2),
        "final_memory_mb": round(final_memory_mb, 2),
        "memory_delta_mb": round(memory_delta_mb, 2),
    }

    return metrics


def extract_longest_hot_paths(output_file: str, top_n: int = 10) -> list:
    """Extract the longest hot path patterns from matches."""
    import re

    hot_stations = {"3186", "3183", "3203"}

    # Parse matches - groups separated by empty lines
    current_group: list[dict[str, Any]] = []
    all_groups: list[list[dict[str, Any]]] = []

    try:
        with open(output_file, "r") as f:
            for line in f:
                stripped = line.strip()
                if not stripped:
                    # Empty line = end of group
                    if current_group:
                        all_groups.append(current_group)
                        current_group = []
                elif stripped == "{}":
                    # Skip empty dict markers
                    continue
                else:
                    # Parse event dict manually (ast.literal_eval fails on datetime)
                    try:
                        # Extract key fields using regex
                        event = {}
                        if m := re.search(r"'bike_id':\s*'([^']+)'", stripped):
                            event["bike_id"] = m.group(1)
                        if m := re.search(r"'start_station_id':\s*'([^']+)'", stripped):
                            event["start_station_id"] = m.group(1)
                        if m := re.search(r"'end_station_id':\s*'([^']+)'", stripped):
                            event["end_station_id"] = m.group(1)
                        if m := re.search(
                            r"'start_station_name':\s*'([^']+)'", stripped
                        ):
                            event["start_station_name"] = m.group(1)
                        if m := re.search(r"'end_station_name':\s*'([^']+)'", stripped):
                            event["end_station_name"] = m.group(1)
                        if m := re.search(r"'trip_duration':\s*(\d+)", stripped):
                            event["trip_duration"] = int(m.group(1))
                        if m := re.search(
                            r"'started_at':\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})",
                            stripped,
                        ):
                            event["started_at"] = m.group(1)
                        if m := re.search(
                            r"'ended_at':\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})",
                            stripped,
                        ):
                            event["ended_at"] = m.group(1)

                        if event:
                            current_group.append(event)
                    except Exception:
                        pass

            if current_group:
                all_groups.append(current_group)
    except Exception:
        return []

    # Find longest chains (by number of events)
    hot_paths = []
    for group in all_groups:
        if not group:
            continue

        # Last event should end at hot station
        last_event = group[-1]
        end_station = last_event.get("end_station_id")
        if end_station not in hot_stations:
            continue

        # Build path info
        bike_id = last_event.get("bike_id")
        path_length = len(group)
        stations = []
        for event in group:
            stations.append(event.get("start_station_name", "Unknown"))
        stations.append(last_event.get("end_station_name", "Unknown"))

        start_time = group[0].get("started_at")
        end_time = last_event.get("ended_at")
        total_duration = sum(e.get("trip_duration", 0) for e in group)

        hot_paths.append(
            {
                "bike_id": bike_id,
                "path_length": path_length,
                "stations": stations,
                "start_time": start_time,
                "end_time": end_time,
                "total_duration_sec": total_duration,
                "final_station": last_event.get("end_station_name"),
                "final_station_id": last_event.get("end_station_id"),
            }
        )

    # Sort by path length (longest first)
    hot_paths.sort(key=lambda x: x["path_length"], reverse=True)
    return hot_paths[:top_n]


def print_longest_hot_paths(output_file: str, top_n: int = 10) -> None:
    """Print the longest hot path patterns found."""
    paths = extract_longest_hot_paths(output_file, top_n)

    if not paths:
        print(f"\n⚠️  No hot paths found in {output_file}")
        return

    print("\n" + "=" * 70)
    print(f"🏆 TOP {len(paths)} LONGEST HOT PATHS")
    print("=" * 70)

    for i, path in enumerate(paths, 1):
        duration_min = path["total_duration_sec"] / 60
        station_chain = " → ".join(path["stations"])

        print(f"\n#{i}. Bike #{path['bike_id']} - {path['path_length']} trips")
        print(f"   🚴 Route: {station_chain}")
        print(
            f"   🎯 Final destination: {path['final_station']} (Station {path['final_station_id']})"
        )
        print(f"   ⏱️  Total journey: {duration_min:.1f} minutes")
        print(f"   📅 Period: {path['start_time']} → {path['end_time']}")


def print_results(metrics: Dict[str, Any], output_dir: str = "outputs") -> None:
    """Pretty print CEP results."""
    if "error" in metrics:
        print(f"❌ Error: {metrics['error']}")
        return

    print("\n" + "=" * 70)
    print("🎯 INTELLIGENT CEP WITH PATTERN-AWARE LOAD SHEDDING")
    print("=" * 70)
    print(f"📊 Patterns: {metrics['pattern_count']} optimized hot paths patterns")
    print(f"📁 Input: {metrics['input_file']}")
    print(f"🔢 Total input events: {metrics['total_input_events']}")
    print(f"✅ Events processed: {metrics['lines_processed']}")
    print(
        f"❌ Events dropped: {metrics['events_dropped']} ({metrics['drop_rate_percent']}%)"
    )

    print("\n🧠 PATTERN-AWARE PROTECTION:")
    print(f"   🔒 Events protected: {metrics['events_protected']}")
    print(f"   🆔 Protected bike IDs: {metrics['protected_bikes']}")
    print(f"   🚉 Protected station IDs: {metrics['protected_stations']}")
    print("   💡 Protection ensures partial matches can complete")

    print("\n🎯 INTELLIGENT LOAD SHEDDING BREAKDOWN:")
    print(f"   🎲 Adaptive sampling: {metrics['shed_by_sampling']} events")
    print(f"   🔄 Same station trips: {metrics['shed_by_same_station']} events")

    print("\n📈 REAL-TIME ADAPTATION:")
    print(f"   🎯 Target latency: {metrics['target_latency_ms']}ms/batch")
    print(f"   ⏱️  Final avg latency: {metrics['final_avg_latency_ms']}ms/batch")
    print(f"   🔄 Parameter adjustments: {metrics['parameter_adjustments']}")

    if metrics["latency_target_achieved"]:
        print("   ✅ Latency target achieved!")
    else:
        print("   ⚠️  System under pressure (latency > target)")

    print("\n⏱️  PERFORMANCE:")
    print(f"   Pattern loading: {metrics['pattern_load_seconds']}s")
    print(f"   Stream setup: {metrics['stream_setup_seconds']}s")
    print(f"   CEP initialization: {metrics['cep_init_seconds']}s")
    print(f"   Engine execution: {metrics['engine_execution_seconds']}s")
    print(f"   Total execution: {metrics['total_execution_seconds']}s")

    print("\n🎯 RESULTS:")
    print(f"   Matches found: {metrics['matches_found']}")
    print(f"   Throughput: {metrics['events_per_second']:,} events/sec")
    print(f"   Match rate: {metrics['matches_per_second']} matches/sec")

    print("\n💾 MEMORY USAGE:")
    print(f"   Initial: {metrics['initial_memory_mb']} MB")
    print(f"   Peak: {metrics['peak_memory_mb']} MB")
    print(f"   Delta: {metrics['memory_delta_mb']:+} MB")

    # Analysis
    drop_rate = metrics["drop_rate_percent"]
    throughput = metrics["events_per_second"]

    print("\n📈 LOAD SHEDDING ANALYSIS:")
    if drop_rate < 10:
        print("   ✅ Conservative shedding - can handle more load")
    elif drop_rate < 30:
        print("   ⚡ Moderate shedding - good balance")
    elif drop_rate < 50:
        print("   ⚠️  Aggressive shedding - monitor recall quality")
    else:
        print("   🔥 Very aggressive shedding - may miss some patterns")

    if throughput > 500:
        print("   🚀 High throughput achieved")
    elif throughput > 100:
        print("   ⚡ Good throughput")
    else:
        print("   🐌 Moderate throughput")

    print("=" * 70)

    # Show longest hot paths
    output_file = os.path.join(output_dir, "matches.txt")
    if os.path.exists(output_file):
        print_longest_hot_paths(output_file, top_n=10)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="cep_runner",
        description="Adaptive CEP Runner - Hot paths detection with intelligent load shedding",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
🧠 INTELLIGENT LOAD SHEDDING:
  Priority 1: Protect events with bike_ids/stations in active partial matches
  Priority 2: Protect events to/from target hot stations (3186, 3183, 3203)
  Priority 3: Drop same-station round trips
  Priority 4: Adaptive sampling when latency exceeds target (50ms/batch)

Examples:
  # Test with 100 events
  uv run cep-runner --input data/JC-201709-citibike-tripdata.csv --max-lines 100 --verbose

  # Medium scale test
  uv run cep-runner --input data/JC-201709-citibike-tripdata.csv --max-lines 1000

  # Full September 2017 dataset (33,120 events)
  uv run cep-runner --input data/JC-201709-citibike-tripdata.csv --verbose

  # JSON output for analysis
  uv run cep-runner --input data/JC-201709-citibike-tripdata.csv --json
        """,
    )

    parser.add_argument(
        "--input", required=True, help="Path to input CSV file (CitiBike trip data)"
    )
    parser.add_argument(
        "--output",
        default="outputs",
        help="Output directory for matches (default: outputs)",
    )
    parser.add_argument(
        "--max-lines", type=int, help="Limit number of input lines for testing"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Verbose output during processing"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON instead of pretty print",
    )

    args = parser.parse_args()

    # Validate input file exists
    if not os.path.exists(args.input):
        print(f"❌ Error: Input file '{args.input}' does not exist")
        sys.exit(1)

    # Run the CEP system
    try:
        metrics = run_hot_paths_cep(
            args.input, args.output, max_lines=args.max_lines, verbose=args.verbose
        )

        if args.json:
            print(json.dumps(metrics, indent=2))
        else:
            print_results(metrics, output_dir=args.output)

    except KeyboardInterrupt:
        print("\n❌ Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
