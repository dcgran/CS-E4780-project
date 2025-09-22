from datetime import timedelta
from opencep.base.Pattern import Pattern
from opencep.base.PatternStructure import PrimitiveEventStructure, SeqOperator
from opencep.condition.BaseRelationCondition import SmallerThanCondition
from opencep.condition.CompositeCondition import AndCondition
from opencep.condition.Condition import Variable


def main() -> None:
    googleAscendPattern = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), 
                    PrimitiveEventStructure("GOOG", "b"), 
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]), 
                                 Variable("b", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]), 
                                 Variable("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    print(googleAscendPattern)


if __name__ == "__main__":
    main()
