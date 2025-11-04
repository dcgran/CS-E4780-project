"""Training dataset for Graph RAG few-shot learning."""

import dspy

EXAMPLE_SCHEMA = """{
  "nodes": [
    {"label": "Scholar", "properties": [{"name": "knownName", "type": "STRING"}]},
    {"label": "Prize", "properties": [{"name": "category", "type": "STRING"}, {"name": "year", "type": "INT64"}]},
    {"label": "Institution", "properties": [{"name": "name", "type": "STRING"}]},
    {"label": "City", "properties": [{"name": "name", "type": "STRING"}]},
    {"label": "Country", "properties": [{"name": "name", "type": "STRING"}]}
  ],
  "edges": [
    {"label": "WON", "from": "Scholar", "to": "Prize"},
    {"label": "AFFILIATED_WITH", "from": "Scholar", "to": "Institution"},
    {"label": "BORN_IN", "from": "Scholar", "to": "City"},
    {"label": "IS_LOCATED_IN", "from": "Institution", "to": "City"},
    {"label": "IS_CITY_IN", "from": "City", "to": "Country"}
  ]
}"""


def get_trainset() -> list[dspy.Example]:
    trainset = [
        dspy.Example(
            question="Which scholars won prizes in Physics?",
            input_schema=EXAMPLE_SCHEMA,
            query="MATCH (s:Scholar)-[r:WON]->(p:Prize) WHERE lower(p.category) CONTAINS 'physics' RETURN s.knownName",
            context="['Albert Einstein', 'Marie Curie', 'Niels Bohr']",
            answer="Several scholars have won prizes in Physics, including Albert Einstein, Marie Curie, and Niels Bohr.",
        ).with_inputs("question", "input_schema"),
        dspy.Example(
            question="Which scholars were affiliated with University of Cambridge?",
            input_schema=EXAMPLE_SCHEMA,
            query="MATCH (s:Scholar)-[r:AFFILIATED_WITH]->(i:Institution) WHERE lower(i.name) CONTAINS 'cambridge' RETURN s.knownName",
            context="['James Watson', 'Francis Crick', 'Ernest Rutherford']",
            answer="Scholars affiliated with the University of Cambridge include James Watson, Francis Crick, and Ernest Rutherford.",
        ).with_inputs("question", "input_schema"),
        dspy.Example(
            question="Which scholars won prizes in Chemistry and were affiliated with MIT?",
            input_schema=EXAMPLE_SCHEMA,
            query="MATCH (s:Scholar)-[r1:WON]->(p:Prize), (s)-[r2:AFFILIATED_WITH]->(i:Institution) WHERE lower(p.category) CONTAINS 'chemistry' AND lower(i.name) CONTAINS 'mit' RETURN s.knownName",
            context="['Richard R. Schrock', 'Robert H. Grubbs']",
            answer="Scholars who won Chemistry prizes and were affiliated with MIT include Richard R. Schrock and Robert H. Grubbs.",
        ).with_inputs("question", "input_schema"),
        dspy.Example(
            question="Which scholars were born in Paris?",
            input_schema=EXAMPLE_SCHEMA,
            query="MATCH (s:Scholar)-[r:BORN_IN]->(c:City) WHERE lower(c.name) CONTAINS 'paris' RETURN s.knownName",
            context="['Marie Curie', 'Louis de Broglie', 'Henri Becquerel']",
            answer="Scholars born in Paris include Marie Curie, Louis de Broglie, and Henri Becquerel.",
        ).with_inputs("question", "input_schema"),
        dspy.Example(
            question="Which institutions are located in the United States?",
            input_schema=EXAMPLE_SCHEMA,
            query="MATCH (i:Institution)-[r1:IS_LOCATED_IN]->(c:City)-[r2:IS_CITY_IN]->(co:Country) WHERE lower(co.name) CONTAINS 'united states' RETURN i.name",
            context="['Harvard University', 'MIT', 'Stanford University', 'University of California, Berkeley']",
            answer="Institutions located in the United States include Harvard University, MIT, Stanford University, and the University of California, Berkeley.",
        ).with_inputs("question", "input_schema"),
        dspy.Example(
            question="How many scholars won prizes in Literature?",
            input_schema=EXAMPLE_SCHEMA,
            query="MATCH (s:Scholar)-[r:WON]->(p:Prize) WHERE lower(p.category) CONTAINS 'literature' RETURN COUNT(DISTINCT s)",
            context="[42]",
            answer="42 scholars have won prizes in Literature.",
        ).with_inputs("question", "input_schema"),
        dspy.Example(
            question="What prizes did Marie Curie win?",
            input_schema=EXAMPLE_SCHEMA,
            query="MATCH (s:Scholar)-[r:WON]->(p:Prize) WHERE lower(s.knownName) CONTAINS 'marie curie' RETURN p.category, p.year",
            context="[('Physics', 1903), ('Chemistry', 1911)]",
            answer="Marie Curie won prizes in Physics in 1903 and Chemistry in 1911.",
        ).with_inputs("question", "input_schema"),
        dspy.Example(
            question="Which scholars won prizes in 2020?",
            input_schema=EXAMPLE_SCHEMA,
            query="MATCH (s:Scholar)-[r:WON]->(p:Prize) WHERE p.year = 2020 RETURN s.knownName, p.category",
            context="[('Andrea Ghez', 'Physics'), ('Jennifer Doudna', 'Chemistry'), ('Louise Glück', 'Literature')]",
            answer="In 2020, Andrea Ghez won in Physics, Jennifer Doudna won in Chemistry, and Louise Glück won in Literature.",
        ).with_inputs("question", "input_schema"),
        dspy.Example(
            question="List the institutions that Physics laureates were affiliated with",
            input_schema=EXAMPLE_SCHEMA,
            query="MATCH (s:Scholar)-[r1:WON]->(p:Prize), (s)-[r2:AFFILIATED_WITH]->(i:Institution) WHERE lower(p.category) CONTAINS 'physics' RETURN DISTINCT i.name",
            context="['University of Cambridge', 'Princeton University', 'ETH Zurich', 'University of Copenhagen']",
            answer="Physics laureates were affiliated with institutions including the University of Cambridge, Princeton University, ETH Zurich, and the University of Copenhagen.",
        ).with_inputs("question", "input_schema"),
        dspy.Example(
            question="How many scholars were born in Germany?",
            input_schema=EXAMPLE_SCHEMA,
            query="MATCH (s:Scholar)-[r1:BORN_IN]->(c:City)-[r2:IS_CITY_IN]->(co:Country) WHERE lower(co.name) CONTAINS 'germany' RETURN COUNT(DISTINCT s)",
            context="[89]",
            answer="89 scholars were born in Germany.",
        ).with_inputs("question", "input_schema"),
        dspy.Example(
            question="Which Chemistry scholars were affiliated with institutions in France?",
            input_schema=EXAMPLE_SCHEMA,
            query="MATCH (s:Scholar)-[r1:WON]->(p:Prize), (s)-[r2:AFFILIATED_WITH]->(i:Institution)-[r3:IS_LOCATED_IN]->(c:City)-[r4:IS_CITY_IN]->(co:Country) WHERE lower(p.category) CONTAINS 'chemistry' AND lower(co.name) CONTAINS 'france' RETURN s.knownName",
            context="['Marie Curie', 'Jean-Pierre Sauvage']",
            answer="Chemistry scholars affiliated with institutions in France include Marie Curie and Jean-Pierre Sauvage.",
        ).with_inputs("question", "input_schema"),
        dspy.Example(
            question="Which scholars won prizes in Underwater Basket Weaving?",
            input_schema=EXAMPLE_SCHEMA,
            query="MATCH (s:Scholar)-[r:WON]->(p:Prize) WHERE lower(p.category) CONTAINS 'underwater basket weaving' RETURN s.knownName",
            context="[]",
            answer="I don't have enough information to answer this question. There are no prizes in that category in the database.",
        ).with_inputs("question", "input_schema"),
        dspy.Example(
            question="Where is Stanford University located?",
            input_schema=EXAMPLE_SCHEMA,
            query="MATCH (i:Institution)-[r:IS_LOCATED_IN]->(c:City) WHERE lower(i.name) CONTAINS 'stanford' RETURN c.name",
            context="['Stanford']",
            answer="Stanford University is located in Stanford.",
        ).with_inputs("question", "input_schema"),
        dspy.Example(
            question="List all prize categories",
            input_schema=EXAMPLE_SCHEMA,
            query="MATCH (p:Prize) RETURN DISTINCT p.category",
            context="['Physics', 'Chemistry', 'Medicine', 'Literature', 'Peace', 'Economics']",
            answer="The prize categories are Physics, Chemistry, Medicine, Literature, Peace, and Economics.",
        ).with_inputs("question", "input_schema"),
        dspy.Example(
            question="Who won Economics prizes after 2015?",
            input_schema=EXAMPLE_SCHEMA,
            query="MATCH (s:Scholar)-[r:WON]->(p:Prize) WHERE lower(p.category) CONTAINS 'economics' AND p.year > 2015 RETURN s.knownName, p.year",
            context="[('Richard Thaler', 2017), ('William Nordhaus', 2018), ('Abhijit Banerjee', 2019)]",
            answer="Economics prize winners after 2015 include Richard Thaler in 2017, William Nordhaus in 2018, and Abhijit Banerjee in 2019.",
        ).with_inputs("question", "input_schema"),
    ]

    return trainset


def get_validation_set() -> list[dspy.Example]:
    validation_set = [
        dspy.Example(
            question="Which scholars won prizes in Medicine?",
            input_schema=EXAMPLE_SCHEMA,
            query="MATCH (s:Scholar)-[r:WON]->(p:Prize) WHERE lower(p.category) CONTAINS 'medicine' RETURN s.knownName",
            answer="Scholars who won prizes in Medicine include...",
        ).with_inputs("question", "input_schema"),
        dspy.Example(
            question="How many scholars were affiliated with Harvard University?",
            input_schema=EXAMPLE_SCHEMA,
            query="MATCH (s:Scholar)-[r:AFFILIATED_WITH]->(i:Institution) WHERE lower(i.name) CONTAINS 'harvard' RETURN COUNT(DISTINCT s)",
            answer="The number of scholars affiliated with Harvard is...",
        ).with_inputs("question", "input_schema"),
        dspy.Example(
            question="Which Peace prize winners were born in Sweden?",
            input_schema=EXAMPLE_SCHEMA,
            query="MATCH (s:Scholar)-[r1:WON]->(p:Prize), (s)-[r2:BORN_IN]->(c:City)-[r3:IS_CITY_IN]->(co:Country) WHERE lower(p.category) CONTAINS 'peace' AND lower(co.name) CONTAINS 'sweden' RETURN s.knownName",
            answer="Peace prize winners born in Sweden include...",
        ).with_inputs("question", "input_schema"),
    ]

    return validation_set


if __name__ == "__main__":
    trainset = get_trainset()
    print(f"Training set size: {len(trainset)}")
    print("\nFirst example:")
    print(f"  Question: {trainset[0].question}")
    print(f"  Query: {trainset[0].query}")
    print(f"  Answer: {trainset[0].answer}")

    validation_set = get_validation_set()
    print(f"\nValidation set size: {len(validation_set)}")
