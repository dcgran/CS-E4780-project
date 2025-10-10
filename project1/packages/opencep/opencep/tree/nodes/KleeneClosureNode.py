from functools import reduce
from opencep.misc.Utils import calculate_joint_probability

from opencep.base.Event import Event, AggregatedEvent
from opencep.condition.CompositeCondition import CompositeCondition
from opencep.tree.nodes.Node import Node, PatternParameters
from opencep.tree.nodes.UnaryNode import UnaryNode


class KleeneClosureNode(UnaryNode):
    """
    An internal node representing a Kleene closure operator.
    It generates and propagates sets of partial matches provided by its sole child.
    """
    def __init__(self, pattern_params: PatternParameters, min_size, max_size,
                 parents: list[Node] | None = None, pattern_ids: int | set[int] | None = None):
        super().__init__(pattern_params, parents, pattern_ids)
        self.__min_size = min_size
        self.__max_size = max_size

    def handle_new_partial_match(self, partial_match_source: Node):
        """
        Reacts upon a notification of a new partial match available at the child by generating, validating,
        and propagating sets of partial matches containing this new partial match.
        Note: this method strictly assumes that the last partial match in the child storage is the one to cause the
        method call (could not function properly in a parallelized implementation of the evaluation tree).
        """
        if self._child is None:
            raise Exception()  # should never happen

        new_partial_match = self._child.get_last_unhandled_partial_match_by_parent(self)

        if self._partial_matches:
            cutoff_timestamp = float(new_partial_match.last_timestamp) - float(self._sliding_window.total_seconds())
            self._partial_matches._clean_expired_partial_matches(cutoff_timestamp)

        # create partial match sets containing the new partial match that triggered this method
        child_matches_powerset = self.__create_child_matches_powerset(new_partial_match)

        for partial_match_set in child_matches_powerset:
            # create and propagate the new match
            all_primitive_events = reduce(lambda x, y: x + y, [pm.events for pm in partial_match_set])
            probability = None if self._confidence is None else \
                reduce(calculate_joint_probability, (pm.probability for pm in partial_match_set), None)
            aggregated_event = AggregatedEvent(all_primitive_events, probability)
            self._validate_and_propagate_partial_match([aggregated_event], probability)

    def _validate_new_match(self, events_for_new_match: list[Event]):
        """
        Validates the condition stored in this node on the given set of events.
        """
        if len(events_for_new_match) != 1 or not isinstance(events_for_new_match[0], AggregatedEvent):
            raise Exception("Unexpected candidate event list for Kleene closure operator")
        if not Node._validate_new_match(self, events_for_new_match):
            return False
        return self._condition.eval([e.payload for e in events_for_new_match[0].primitive_events])

    def __create_child_matches_powerset(self, new_partial_match):
        """
        Generates subsets of partial matches using indexed retrieval and temporal validation.
        """
        all_child_matches = self._child.get_partial_matches()
        filter_value = self.__extract_grouping_value_for_indexed_retrieval(new_partial_match)
        if filter_value is not None:
            from opencep.condition.KCCondition import KCIndexCondition
            if self._condition and hasattr(self._condition, 'get_conditions_list'):
                kc_conditions = [c for c in self._condition.get_conditions_list() if isinstance(c, KCIndexCondition)]
                grouping_condition = next((c for c in kc_conditions if c.get_offset() == 1), None)
                if grouping_condition:
                    grouping_index = {}
                    cutoff_timestamp = float(new_partial_match.last_timestamp) - float(self._sliding_window.total_seconds())
                    for pm in all_child_matches:
                        if pm.first_timestamp < cutoff_timestamp:
                            continue
                        if hasattr(pm, 'events') and len(pm.events) > 0 and hasattr(pm.events[0], 'payload'):
                            grouping_value = grouping_condition._getattr_func(pm.events[0].payload)
                            if grouping_value not in grouping_index:
                                grouping_index[grouping_value] = []
                            grouping_index[grouping_value].append(pm)

                    child_partial_matches = grouping_index.get(filter_value, [])
                else:
                    child_partial_matches = all_child_matches
            else:
                child_partial_matches = all_child_matches
        else:
            child_partial_matches = all_child_matches

        if len(child_partial_matches) == 0:
            return []

        last_partial_match = child_partial_matches[-1]
        actual_max_size = self.__max_size if self.__max_size is not None else len(child_partial_matches)
        filtered_matches = child_partial_matches
        result_powerset = []

        for seq_length in range(min(actual_max_size, len(filtered_matches)), 0, -1):
            sequence = filtered_matches[-seq_length:]
            if (sequence[-1] == last_partial_match and
                len(sequence) >= self.__min_size and
                self.__is_sequence_temporally_valid(sequence)):
                result_powerset.append(sequence)

        return result_powerset

    def __extract_grouping_value_for_indexed_retrieval(self, triggering_match):
        if not self._condition or not hasattr(self._condition, 'get_conditions_list'):
            return None

        from opencep.condition.KCCondition import KCIndexCondition
        kc_conditions = [c for c in self._condition.get_conditions_list() if isinstance(c, KCIndexCondition)]
        grouping_condition = next((c for c in kc_conditions if c.get_offset() == 1), None)

        if not grouping_condition:
            return None

        if not hasattr(triggering_match, 'events') or len(triggering_match.events) == 0:
            return None

        recent_event = triggering_match.events[0]
        if not hasattr(recent_event, 'payload'):
            return None

        return grouping_condition._getattr_func(recent_event.payload)

    def __is_sequence_temporally_valid(self, sequence):
        if not sequence:
            return False
        time_diff = sequence[-1].last_timestamp - sequence[0].first_timestamp
        return time_diff <= self._sliding_window.total_seconds()

    def clean_expired_partial_matches(self, last_timestamp):
        if not Node._is_partial_match_expiration_enabled():
            return
        cutoff_timestamp = float(last_timestamp) - float(self._sliding_window.total_seconds())
        if self._partial_matches:
            self._partial_matches._clean_expired_partial_matches(cutoff_timestamp)

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
