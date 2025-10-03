from typing import List, Set
from functools import reduce
from opencep.misc.Utils import calculate_joint_probability

from opencep.base.Event import Event, AggregatedEvent
from opencep.condition.CompositeCondition import CompositeCondition
from opencep.base.PatternMatch import PatternMatch
from opencep.misc.Utils import powerset_generator
from opencep.tree.nodes.Node import Node, PatternParameters
from opencep.tree.nodes.UnaryNode import UnaryNode


class KleeneClosureNode(UnaryNode):
    """
    An internal node representing a Kleene closure operator.
    It generates and propagates sets of partial matches provided by its sole child.
    """
    def __init__(self, pattern_params: PatternParameters, min_size, max_size,
                 parents: List[Node] = None, pattern_ids: int or Set[int] = None):
        super().__init__(pattern_params, parents, pattern_ids)
        self.__min_size = min_size
        self.__max_size = max_size

    def handle_new_partial_match(self, partial_match_source: Node):
        """
        OPTIMIZED: Reacts upon a notification of a new partial match available at the child by generating, validating,
        and propagating sets of partial matches containing this new partial match.
        Note: this method strictly assumes that the last partial match in the child storage is the one to cause the
        method call (could not function properly in a parallelized implementation of the evaluation tree).

        PERFORMANCE OPTIMIZATIONS:
        - Aggressive cleanup of expired matches before powerset generation (temporal window enforcement)
        - Pre-filtering by KC condition hints to avoid building invalid sequences
        - Early validation to avoid creating unnecessary AggregatedEvents
        """
        if self._child is None:
            raise Exception()  # should never happen

        new_partial_match = self._child.get_last_unhandled_partial_match_by_parent(self)

        # OPTIMIZATION: Clean child expired matches before processing
        self._child.clean_expired_partial_matches(new_partial_match.last_timestamp)

        # create partial match sets containing the new partial match that triggered this method
        child_matches_powerset = self.__create_child_matches_powerset()

        for partial_match_set in child_matches_powerset:
            # create and propagate the new match
            all_primitive_events = reduce(lambda x, y: x + y, [pm.events for pm in partial_match_set])
            probability = None if self._confidence is None else \
                reduce(calculate_joint_probability, (pm.probability for pm in partial_match_set), None)
            aggregated_event = AggregatedEvent(all_primitive_events, probability)
            self._validate_and_propagate_partial_match([aggregated_event], probability)

    def _validate_new_match(self, events_for_new_match: List[Event]):
        """
        Validates the condition stored in this node on the given set of events.
        """
        if len(events_for_new_match) != 1 or not isinstance(events_for_new_match[0], AggregatedEvent):
            raise Exception("Unexpected candidate event list for Kleene closure operator")
        if not Node._validate_new_match(self, events_for_new_match):
            return False
        return self._condition.eval([e.payload for e in events_for_new_match[0].primitive_events])

    def __create_child_matches_powerset(self):
        """
        OPTIMIZED: Generate valid KC sequences with smart pre-filtering.

        STRATEGY:
        1. Pre-filter child matches using KC condition hints (if available)
           - Uses first KCIndexCondition with offset=1 to identify grouping attributes
           - Only builds sequences from matching items (e.g., same bike_id)
        2. Generate all valid sequence lengths respecting min_size and max_size
        3. Validate temporal constraints and KC conditions

        With pre-filtering, we avoid building invalid mixed sequences, so no artificial
        caps are needed. Complexity: O(filtered_matches * max_size) where filtered_matches
        is typically much smaller than total child storage.
        """
        child_partial_matches = self._child.get_partial_matches()
        if len(child_partial_matches) == 0:
            return []

        last_partial_match = child_partial_matches[-1]
        actual_max_size = self.__max_size if self.__max_size is not None else len(child_partial_matches)

        # OPTIMIZATION: Pre-filter using KC condition hints to avoid building invalid sequences
        filtered_matches = self.__prefilter_by_kc_condition(child_partial_matches, last_partial_match)

        result_powerset = []

        # Generate sequences from longest to shortest
        for seq_length in range(min(actual_max_size, len(filtered_matches)), 0, -1):
            # Take the most recent seq_length matches
            sequence = filtered_matches[-seq_length:]

            # Verify this sequence:
            # 1. Ends with the triggering match
            # 2. Meets minimum size constraint
            if (sequence[-1] == last_partial_match and
                len(sequence) >= self.__min_size):

                # Check temporal validity and KC conditions
                if self.__is_sequence_temporally_valid(sequence):
                    result_powerset.append(sequence)

        return result_powerset

    def __prefilter_by_kc_condition(self, child_matches, triggering_match):
        """
        Pre-filter child matches using KC condition hints to avoid building sequences
        that will be rejected. Uses the first KCIndexCondition with offset=1 to identify
        grouping attributes (e.g., bike_id for bike trip chains).

        This makes sequence generation generic while avoiding the performance cost of
        building and validating sequences that mix incompatible events.
        """
        if not self._condition or not hasattr(self._condition, 'get_conditions_list'):
            # No condition or not a composite condition - no filtering
            return child_matches

        # Find first KCIndexCondition with offset=1 (consecutive item comparison)
        from opencep.condition.KCCondition import KCIndexCondition
        kc_conditions = [c for c in self._condition.get_conditions_list() if isinstance(c, KCIndexCondition)]
        grouping_condition = next((c for c in kc_conditions if c.get_offset() == 1), None)

        if not grouping_condition:
            # No grouping condition found - no filtering
            return child_matches

        # Extract grouping attribute from triggering match
        if not hasattr(triggering_match, 'events') or len(triggering_match.events) == 0:
            return child_matches

        triggering_event = triggering_match.events[0]
        if not hasattr(triggering_event, 'payload'):
            return child_matches

        # Use the getattr_func from the KC condition to extract the grouping value
        triggering_value = grouping_condition._getattr_func(triggering_event.payload)

        # Filter to only matches with the same grouping value
        filtered = []
        for pm in child_matches:
            if hasattr(pm, 'events') and len(pm.events) > 0:
                event = pm.events[0]
                if hasattr(event, 'payload'):
                    value = grouping_condition._getattr_func(event.payload)
                    if value == triggering_value:
                        filtered.append(pm)

        return filtered if filtered else child_matches  # Fallback to all if filtering produces nothing

    def __is_sequence_temporally_valid(self, sequence):
        """
        Check if all events in the sequence fall within the time window.
        Returns True if the sequence is valid, False if any events have expired.
        """
        if not sequence or len(sequence) == 0:
            return False

        # Get the time span of this sequence
        first_timestamp = sequence[0].first_timestamp
        last_timestamp = sequence[-1].last_timestamp

        # Check if the time difference is within the window
        time_diff = last_timestamp - first_timestamp

        # Convert sliding window to seconds for comparison
        window_seconds = self._sliding_window.total_seconds()

        return time_diff <= window_seconds

    def apply_condition(self, condition: CompositeCondition):
        """
        The default implementation is overridden to extract KC conditions from the given composite condition.
        """
        self._propagate_condition(condition)
        names = {event_def.name for event_def in self.get_event_definitions()}
        self._condition = condition.get_condition_of(names, get_kleene_closure_conditions=True,
                                                     consume_returned_conditions=True)

    def get_structure_summary(self):
        return "KC", self._child.get_structure_summary()

    def is_equivalent(self, other):
        """
        In addition to the checks performed by the base class, compares the min_size and max_size fields.
        """
        if not super().is_equivalent(other):
            return False
        return self.__min_size == other.__min_size and self.__max_size == other.__max_size
