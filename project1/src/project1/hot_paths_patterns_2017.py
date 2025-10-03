"""Hot paths pattern for 2017 CitiBike data with real bike IDs.

Implements: PATTERN SEQ (BikeTrip+ a[], BikeTrip b)
WHERE a[last].bike = b.bike AND b.end in {hot_stations}
WITHIN 1 hour

Using the TOP 3 hottest stations from 2017 data analysis:
- Station 3186: 15.3% of trips (Grove St PATH)
- Station 3183: 8.0% of trips (Exchange Place)
- Station 3203: 5.7% of trips (Hamilton Park)
"""

from typing import List
from datetime import timedelta

from opencep.base.Pattern import Pattern
from opencep.base.PatternStructure import (
    PrimitiveEventStructure,
    SeqOperator,
    KleeneClosureOperator,
)
from opencep.condition.Condition import Variable, BinaryCondition
from opencep.condition.CompositeCondition import AndCondition
from opencep.condition.KCCondition import KCIndexCondition
from opencep.misc.ConsumptionPolicy import ConsumptionPolicy
from opencep.misc.SelectionStrategies import SelectionStrategies


def create_2017_hot_paths_patterns() -> List[Pattern]:
    hot_stations = {"3186", "3183", "3203"}

    structure = SeqOperator(
        KleeneClosureOperator(
            PrimitiveEventStructure("BikeTrip", "a"), min_size=1, max_size=10
        ),
        PrimitiveEventStructure("BikeTrip", "b"),
    )

    # KC condition: same bike for all consecutive trips (a[i].bike == a[i+1].bike)
    kc_same_bike = KCIndexCondition(
        names={"a"},
        getattr_func=lambda x: x.get("bike_id", ""),
        relation_op=lambda bike1, bike2: bike1 == bike2,
        offset=1,  # Compare each trip with the next one
    )

    # KC condition: station chaining (a[i].end_station == a[i+1].start_station)
    # Use index-based approach: compare trip i's end with trip i+1's start
    kc_station_chain = KCIndexCondition(
        names={"a"},
        getattr_func=lambda x: x,  # Pass the whole event
        relation_op=lambda trip1, trip2: trip1.get("end_station_id", "")
        == trip2.get("start_station_id", ""),
        offset=1,
    )

    # SEQ condition: a[last].bike == b.bike AND a[last].end == b.start AND b.end in hot_stations
    def extract_bike_and_end_station(x):
        """Extract (bike_id, end_station_id) from last event in KC."""
        if isinstance(x, list):
            if not x:
                return (None, None)
            return (x[-1].get("bike_id", ""), x[-1].get("end_station_id", ""))
        return (
            (x.get("bike_id", ""), x.get("end_station_id", ""))
            if hasattr(x, "get")
            else (None, None)
        )

    def extract_bike_start_and_end_station(x):
        return (
            x.get("bike_id", ""),
            x.get("start_station_id", ""),
            x.get("end_station_id", ""),
        )

    seq_condition = BinaryCondition(
        Variable("a", extract_bike_and_end_station),
        Variable("b", extract_bike_start_and_end_station),
        lambda a_tuple, b_tuple: (
            a_tuple[0] is not None  # Valid KC result
            and a_tuple[0]  # Non-empty bike_id
            and a_tuple[0] == b_tuple[0]  # Same bike: a[last].bike == b.bike
            and a_tuple[1] == b_tuple[1]  # Chain continues: a[last].end == b.start
            and b_tuple[2] in hot_stations  # b ends at hot station
        ),
    )

    # Combine all conditions
    condition = AndCondition(kc_same_bike, kc_station_chain, seq_condition)

    pattern = Pattern(
        structure,
        condition,
        timedelta(hours=1),
        ConsumptionPolicy(primary_selection_strategy=SelectionStrategies.MATCH_ANY),
    )

    return [pattern]
