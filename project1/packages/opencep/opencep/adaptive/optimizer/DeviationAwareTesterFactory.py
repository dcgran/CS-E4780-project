from typing import List
from opencep.adaptive.statistics.StatisticsTypes import StatisticsTypes
from opencep.adaptive.optimizer import DeviationAwareTester


class DeviationAwareTesterFactory:
    """
    Creates a deviation aware tester according to the specifications.
    """
    @staticmethod
    def create_deviation_aware_tester(statistics_type: StatisticsTypes or List[StatisticsTypes],
                                      deviation_threshold: float):
        if statistics_type == StatisticsTypes.ARRIVAL_RATES:
            return DeviationAwareTester.ArrivalRatesDeviationAwareTester(deviation_threshold)
        if statistics_type == StatisticsTypes.SELECTIVITY_MATRIX:
            return DeviationAwareTester.SelectivityDeviationAwareOptimizerTester(deviation_threshold)
