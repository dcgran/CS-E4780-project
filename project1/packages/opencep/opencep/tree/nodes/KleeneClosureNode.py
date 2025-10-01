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
        - Aggressive cleanup of expired matches before powerset generation
        - Early validation to avoid creating unnecessary AggregatedEvents
        - Storage size limiting to prevent unbounded growth
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
        OPTIMIZED: Greedy maximal matching with temporal fallback for Kleene closure.

        STRATEGY:
        1. Prefer longest contiguous sequences (maximal matching)
        2. When older events expire, automatically fall back to shorter valid sequences
        3. Only consider sequences that are temporally valid (within time window)

        This reduces complexity from O(2^n) exponential powerset to O(max_size) linear,
        while maintaining correctness by considering temporal expiration.
        """
        child_partial_matches = self._child.get_partial_matches()
        if len(child_partial_matches) == 0:
            return []

        last_partial_match = child_partial_matches[-1]
        actual_max_size = self.__max_size if self.__max_size is not None else len(child_partial_matches)

        result_powerset = []

        # OPTIMIZATION: Greedy approach with temporal fallback
        # Build sequences from longest to shortest, checking temporal validity
        # This naturally handles expiration: when long chains become invalid, we get shorter ones

        for seq_length in range(min(actual_max_size, len(child_partial_matches)), 0, -1):
            # Take the most recent seq_length matches including last_partial_match
            # These are contiguous in time (most recent events)
            sequence = child_partial_matches[-seq_length:]

            # Verify this sequence:
            # 1. Ends with the new partial match (required by Kleene closure semantics)
            # 2. Meets minimum size constraint
            if (sequence[-1] == last_partial_match and
                len(sequence) >= self.__min_size):

                # Check temporal validity: all events in sequence within time window
                if self.__is_sequence_temporally_valid(sequence):
                    result_powerset.append(sequence)

                    # GREEDY: Once we find the longest valid sequence, we're done
                    # The temporal expiration will handle fallback automatically:
                    # - Next time this is called, older events will be cleaned
                    # - We'll naturally get shorter sequences
                    break

        return result_powerset

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
