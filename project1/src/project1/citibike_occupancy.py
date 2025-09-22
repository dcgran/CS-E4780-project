"""Occupancy-aware Citi Bike formatter.

Extends `CitiBikeFormatter` to maintain per-station inferred occupancy (relative counts only)
and annotate each emitted StationDelta event with:
  occupancy_before, occupancy_after

Optionally emits synthetic events when occupancy_after falls below a configured low threshold
or exceeds a high threshold using event_type: LowOccupancy / HighOccupancy for pattern detection.
"""
from __future__ import annotations
from typing import Dict, List, Optional
from dataclasses import dataclass

from .citibike_formatter import CitiBikeFormatter


@dataclass
class OccupancyThresholds:
    low: Optional[int] = None
    high: Optional[int] = None


class OccupancyFormatter(CitiBikeFormatter):
    def __init__(self, collapse_station_delta: bool = True, thresholds: OccupancyThresholds | None = None):
        super().__init__(collapse_station_delta=collapse_station_delta)
        self._counts: Dict[str, int] = {}
        self._thresholds = thresholds or OccupancyThresholds()

    def parse_event(self, raw_data: str):  # type: ignore[override]
        base_events = super().parse_event(raw_data)
        enriched: List[dict] = []
        for ev in base_events:
            if ev.get('event_type') != 'StationDelta':
                enriched.append(ev)
                continue
            station = ev['station_id']
            cur = self._counts.get(station, 0)
            ev['occupancy_before'] = cur
            after = cur + ev['delta']
            ev['occupancy_after'] = after
            self._counts[station] = after
            enriched.append(ev)
            # threshold synthetic events
            if self._thresholds.low is not None and after <= self._thresholds.low:
                enriched.append({
                    'station_id': station,
                    'ts': ev['ts'],
                    'threshold': self._thresholds.low,
                    'occupancy': after,
                    'event_type': 'LowOccupancy'
                })
            if self._thresholds.high is not None and after >= self._thresholds.high:
                enriched.append({
                    'station_id': station,
                    'ts': ev['ts'],
                    'threshold': self._thresholds.high,
                    'occupancy': after,
                    'event_type': 'HighOccupancy'
                })
        return enriched

__all__ = ["OccupancyFormatter", "OccupancyThresholds"]
