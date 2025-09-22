"""
This file contains the main class of the project. It processes streams of events and detects pattern matches
by invoking the rest of the system components.
"""
from opencep.base.DataFormatter import DataFormatter
from opencep.parallel.EvaluationManagerFactory import EvaluationManagerFactory
from opencep.parallel.ParallelExecutionParameters import ParallelExecutionParameters
from opencep.stream.Stream import InputStream, OutputStream
from opencep.base.Pattern import Pattern
from opencep.evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from typing import List
from datetime import datetime
from opencep.transformation.PatternPreprocessingParameters import PatternPreprocessingParameters
from opencep.transformation.PatternPreprocessor import PatternPreprocessor


class CEP:
    """
    A CEP object wraps the engine responsible for actual processing. It accepts the desired workload (list of patterns
    to be evaluated) and a set of settings defining the evaluation mechanism to be used and the way the workload should
    be optimized and parallelized.
    """
    def __init__(self, patterns: Pattern or List[Pattern], eval_mechanism_params: EvaluationMechanismParameters = None,
                 parallel_execution_params: ParallelExecutionParameters = None,
                 pattern_preprocessing_params: PatternPreprocessingParameters = None):
        """
        Constructor of the class.
        """
        actual_patterns = PatternPreprocessor(pattern_preprocessing_params).transform_patterns(patterns)
        self.__evaluation_manager = EvaluationManagerFactory.create_evaluation_manager(actual_patterns,
                                                                                       eval_mechanism_params,
                                                                                       parallel_execution_params)

    def run(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
        Applies the evaluation mechanism to detect the predefined patterns in a given stream of events.
        Returns the total time elapsed during evaluation.
        """
        start = datetime.now()
        self.__evaluation_manager.eval(events, matches, data_formatter)
        return (datetime.now() - start).total_seconds()

    def get_pattern_match(self):
        """
        Returns one match from the output stream.
        """
        try:
            return self.get_pattern_match_stream().get_item()
        except StopIteration:  # the stream might be closed.
            return None

    def get_pattern_match_stream(self):
        """
        Returns the output stream containing the detected matches.
        """
        return self.__evaluation_manager.get_pattern_match_stream()

    def get_evaluation_mechanism_structure_summary(self):
        """
        Returns an object summarizing the structure of the underlying evaluation mechanism.
        """
        return self.__evaluation_manager.get_structure_summary()
