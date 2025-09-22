"""Citi Bike DataFormatter & streaming helpers.

We build two primitive event types:
 - TripStart: fields {ride_id, bike_id, ts, start_station_id, start_station_name, member_casual}
 - TripEnd:   fields {ride_id, bike_id, ts, end_station_id, end_station_name, member_casual}

For pattern simplicity we also optionally collapse into StationDelta events:
 - StationDelta: {station_id, ts, delta, ride_id, bike_id, member_casual}
   where delta = -1 (departure) or +1 (arrival)

Low-latency considerations:
 - Pure Python csv parsing (avoid pandas overhead for per-line ingestion)
 - Streaming generator producing parsed dicts; CEP engine consumes via File-like InputStream wrapper.
 - If scaling further: could memory-map file & custom parse; out of scope now.
"""
from __future__ import annotations
import csv
from datetime import datetime
from typing import Iterable, Dict, Iterator, Optional

from opencep.base.DataFormatter import DataFormatter, EventTypeClassifier

_TS_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

class _TripEventTypeClassifier(EventTypeClassifier):
    def get_event_type(self, event_payload: dict):  # type: ignore[override]
        return event_payload.get("event_type", "Unknown")

class CitiBikeFormatter(DataFormatter):
    def __init__(self, collapse_station_delta: bool = True):
        super().__init__(_TripEventTypeClassifier())
        self._collapse = collapse_station_delta

    def parse_event(self, raw_data: str):  # type: ignore[override]
        # raw_data is a CSV line (already header skipped)
        row = next(csv.reader([raw_data]))
        # columns per provided file header
        (ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id,
         end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual) = row
        if self._collapse:
            # We emit two StationDelta events: departure then arrival.
            start_payload = {
                'ride_id': ride_id,
                'bike_id': rideable_type,  # dataset lacks explicit bike id; substitute rideable_type
                'station_id': start_station_id,
                'station_name': start_station_name,
                'ts': _parse_ts(started_at),
                'delta': -1,
                'member_casual': member_casual,
                'event_type': 'StationDelta'
            }
            end_payload = {
                'ride_id': ride_id,
                'bike_id': rideable_type,
                'station_id': end_station_id,
                'station_name': end_station_name,
                'ts': _parse_ts(ended_at),
                'delta': +1,
                'member_casual': member_casual,
                'event_type': 'StationDelta'
            }
            # For CEP integration we return a list; upstream wrapper will add both.
            return [start_payload, end_payload]
        else:
            return [
                {
                    'ride_id': ride_id,
                    'bike_id': rideable_type,
                    'ts': _parse_ts(started_at),
                    'start_station_id': start_station_id,
                    'start_station_name': start_station_name,
                    'member_casual': member_casual,
                    'event_type': 'TripStart'
                },
                {
                    'ride_id': ride_id,
                    'bike_id': rideable_type,
                    'ts': _parse_ts(ended_at),
                    'end_station_id': end_station_id,
                    'end_station_name': end_station_name,
                    'member_casual': member_casual,
                    'event_type': 'TripEnd'
                }
            ]

    def get_event_timestamp(self, event_payload: dict):  # type: ignore[override]
        return event_payload['ts']


def _parse_ts(s: str) -> datetime:
    return datetime.strptime(s, _TS_FORMAT)


def station_delta_stream(file_path: str, formatter: Optional[CitiBikeFormatter] = None) -> Iterator[dict]:
    fmt = formatter or CitiBikeFormatter(collapse_station_delta=True)
    with open(file_path, 'r') as f:
        header = f.readline()  # skip header
        for line in f:
            line = line.strip()
            if not line:
                continue
            events = fmt.parse_event(line)
            for e in events:
                yield e

__all__ = ["CitiBikeFormatter", "station_delta_stream"]