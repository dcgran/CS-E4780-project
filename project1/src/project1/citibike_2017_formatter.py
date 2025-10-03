"""Data formatter for 2017 CitiBike data with real bike IDs.

Format: tripduration,starttime,stoptime,start station id,start station name,
start station latitude,start station longitude,end station id,end station name,
end station latitude,end station longitude,bikeid,usertype,birth year,gender
"""

from datetime import datetime
from typing import Dict, Any

from opencep.base.DataFormatter import DataFormatter, EventTypeClassifier


class BikeEventClassifier(EventTypeClassifier):
    def get_event_type(self, event_payload: Dict[str, Any]) -> str:
        return "BikeTrip"


class CitiBike2017Formatter(DataFormatter):
    def __init__(self):
        super().__init__(BikeEventClassifier())

    def parse_event(self, raw_data: str) -> Dict[str, Any]:
        parts = [p.strip().strip('"') for p in raw_data.split(",")]

        if len(parts) < 12:
            return {}

        try:
            tripduration = parts[0]
            starttime_str = parts[1]
            stoptime_str = parts[2]
            start_station_id = parts[3]
            start_station_name = parts[4]
            start_station_lat = parts[5]
            start_station_lng = parts[6]
            end_station_id = parts[7]
            end_station_name = parts[8]
            end_station_lat = parts[9]
            end_station_lng = parts[10]
            bike_id = parts[11]

            usertype = parts[12] if len(parts) > 12 else ""
            birth_year = parts[13] if len(parts) > 13 else ""
            gender = parts[14] if len(parts) > 14 else ""

            started_at = datetime.strptime(starttime_str, "%Y-%m-%d %H:%M:%S")
            ended_at = datetime.strptime(stoptime_str, "%Y-%m-%d %H:%M:%S")

            return {
                "trip_duration": int(tripduration) if tripduration.isdigit() else 0,
                "bike_id": bike_id,
                "start_station_id": start_station_id,
                "end_station_id": end_station_id,
                "start_station_name": start_station_name,
                "end_station_name": end_station_name,
                "start_station_lat": float(start_station_lat)
                if start_station_lat
                else 0.0,
                "start_station_lng": float(start_station_lng)
                if start_station_lng
                else 0.0,
                "end_station_lat": float(end_station_lat) if end_station_lat else 0.0,
                "end_station_lng": float(end_station_lng) if end_station_lng else 0.0,
                "started_at": started_at,
                "ended_at": ended_at,
                "usertype": usertype,
                "birth_year": birth_year,
                "gender": gender,
                "event_type": "BikeTrip",
                "duration_minutes": int(tripduration) / 60
                if tripduration.isdigit()
                else 0,
                "is_hot_station_end": end_station_id in ["3186", "3183", "3203"],
            }

        except (ValueError, IndexError):
            return {}

    def get_event_timestamp(self, event_payload: Dict[str, Any]) -> float:
        started_at = event_payload.get("started_at")
        if isinstance(started_at, datetime):
            return started_at.timestamp()
        return 0.0

    def get_event_end_timestamp(self, event_payload: Dict[str, Any]) -> float | None:
        """Return the trip end timestamp for proper duration handling."""
        ended_at = event_payload.get("ended_at")
        if isinstance(ended_at, datetime):
            return ended_at.timestamp()
        return None  # Fall back to start timestamp
