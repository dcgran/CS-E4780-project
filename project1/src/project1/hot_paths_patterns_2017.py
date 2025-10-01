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
from opencep.misc.ConsumptionPolicy import ConsumptionPolicy
from opencep.misc.SelectionStrategies import SelectionStrategies


def create_2017_hot_paths_patterns() -> List[Pattern]:
    hot_stations = {"3186", "3183", "3203"}

    structure = SeqOperator(
        KleeneClosureOperator(
            PrimitiveEventStructure("BikeTrip", "a"), min_size=1, max_size=3
        ),
        PrimitiveEventStructure("BikeTrip", "b"),
    )

    def extract_bike_id_from_kc(x):
        """Handle both list (Kleene closure result) and dict (single event) types."""
        if isinstance(x, list):
            return x[-1].get("bike_id", "") if x else ""
        return x.get("bike_id", "") if hasattr(x, "get") else ""

    def extract_bike_and_station(x):
        return (x.get("bike_id", ""), x.get("end_station_id", ""))

    condition = BinaryCondition(
        Variable("a", extract_bike_id_from_kc),
        Variable("b", extract_bike_and_station),
        lambda bike_a, bike_end_tuple: (
            bike_a and bike_a == bike_end_tuple[0] and bike_end_tuple[1] in hot_stations
        ),
    )

    pattern = Pattern(
        structure,
        condition,
        timedelta(hours=1),
        ConsumptionPolicy(primary_selection_strategy=SelectionStrategies.MATCH_SINGLE),
    )

    return [pattern]
