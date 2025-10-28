import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def __imports() -> tuple:
    import os

    import dspy
    import kuzu
    import marimo as mo
    from dotenv import load_dotenv
    from dspy.adapters.baml_adapter import BAMLAdapter
    from pydantic import BaseModel, Field

    load_dotenv()

    OPENROUTER_API_KEY: str | None = os.environ.get("OPENROUTER_API_KEY")
    return (
        BAMLAdapter,
        BaseModel,
        Field,
        OPENROUTER_API_KEY,
        dspy,
        kuzu,
        mo,
    )


@app.cell
def __title(mo) -> None:
    mo.md(
        r"""
    # Graph RAG using Text2Cypher

    This is a demo app in marimo that allows you to query the Nobel laureate graph (that's managed in Kuzu) using natural language. A language model takes in the question you enter, translates it to Cypher via a custom Text2Cypher pipeline in Kuzu that's powered by DSPy. The response retrieved from the graph database is then used as context to formulate the answer to the question.

    > \- Powered by Kuzu, DSPy and marimo \-
    """
    )


@app.cell
def __text_input(mo) -> tuple:
    text_ui = mo.ui.text(
        value="Which scholars won prizes in Physics and were affiliated with University of Cambridge?",
        full_width=True,
    )
    return (text_ui,)


@app.cell
def __display_text_input(text_ui) -> None:
    text_ui


@app.cell
def __run_query(KuzuDatabaseManager, mo, run_graph_rag, text_ui) -> tuple[str, str]:
    db_name: str = "nobel.kuzu"
    db_manager: KuzuDatabaseManager = KuzuDatabaseManager(db_name)  # type: ignore

    question: str = text_ui.value

    with mo.status.spinner(title="Generating answer...") as _spinner:
        result: dict[str, object] = run_graph_rag([question], db_manager)[0]

    query: str = result["query"]  # type: ignore
    answer: str = result["answer"].response  # type: ignore
    return answer, query


@app.cell
def __display_results(answer, mo, query) -> None:
    mo.hstack(
        [mo.md(f"""### Query\n```{query}```"""), mo.md(f"""### Answer\n{answer}""")]
    )


@app.cell
def __pydantic_models(BaseModel, Field) -> tuple:
    class Query(BaseModel):
        query: str = Field(description="Valid Cypher query with no newlines")

    class Property(BaseModel):
        name: str
        type: str = Field(description="Data type of the property")

    class Node(BaseModel):
        label: str
        properties: list[Property] | None

    class Edge(BaseModel):
        label: str = Field(description="Relationship label")
        from_: Node = Field(alias="from", description="Source node label")
        to: Node = Field(alias="from", description="Target node label")
        properties: list[Property] | None

    class GraphSchema(BaseModel):
        nodes: list[Node]
        edges: list[Edge]

    return GraphSchema, Query


@app.cell
def __dspy_signatures(GraphSchema, Query, dspy) -> tuple:
    class PruneSchema(dspy.Signature):
        """
        Understand the given labelled property graph schema and the given user question. Your task
        is to return ONLY the subset of the schema (node labels, edge labels and properties) that is
        relevant to the question.
            - The schema is a list of nodes and edges in a property graph.
            - The nodes are the entities in the graph.
            - The edges are the relationships between the nodes.
            - Properties of nodes and edges are their attributes, which helps answer the question.
        """

        question: str = dspy.InputField()
        input_schema: str = dspy.InputField()
        pruned_schema: GraphSchema = dspy.OutputField()  # type: ignore

    class Text2Cypher(dspy.Signature):
        """
        Translate the question into a valid Cypher query that respects the graph schema.

        <SYNTAX>
        - When matching on Scholar names, ALWAYS match on the `knownName` property
        - For countries, cities, continents and institutions, you can match on the `name` property
        - Use short, concise alphanumeric strings as names of variable bindings (e.g., `a1`, `r1`, etc.)
        - Always strive to respect the relationship direction (FROM/TO) using the schema information.
        - When comparing string properties, ALWAYS do the following:
            - Lowercase the property values before comparison
            - Use the WHERE clause
            - Use the CONTAINS operator to check for presence of one substring in the other
        - DO NOT use APOC as the database does not support it.
        </SYNTAX>

        <RETURN_RESULTS>
        - If the result is an integer, return it as an integer (not a string).
        - When returning results, return property values rather than the entire node or relationship.
        - Do not attempt to coerce data types to number formats (e.g., integer, float) in your results.
        - NO Cypher keywords should be returned by your query.
        </RETURN_RESULTS>
        """

        question: str = dspy.InputField()
        input_schema: str = dspy.InputField()
        query: Query = dspy.OutputField()  # type: ignore

    class AnswerQuestion(dspy.Signature):
        """
        - Use the provided question, the generated Cypher query and the context to answer the question.
        - If the context is empty, state that you don't have enough information to answer the question.
        - When dealing with dates, mention the month in full.
        """

        question: str = dspy.InputField()
        cypher_query: str = dspy.InputField()
        context: str = dspy.InputField()
        response: str = dspy.OutputField()

    return AnswerQuestion, PruneSchema, Text2Cypher


@app.cell
def __configure_llm(BAMLAdapter, OPENROUTER_API_KEY, dspy) -> None:
    # Using OpenRouter. Switch to another LLM provider as needed
    lm = dspy.LM(
        model="openrouter/google/gemini-2.5-flash",
        api_base="https://openrouter.ai/api/v1",
        api_key=OPENROUTER_API_KEY,
    )
    dspy.configure(lm=lm, adapter=BAMLAdapter())


@app.cell
def __kuzu_manager(kuzu) -> tuple:
    class KuzuDatabaseManager:
        """Manages Kuzu database connection and schema retrieval."""

        def __init__(self, db_path: str = "ldbc_1.kuzu") -> None:
            self.db_path: str = db_path
            self.db = kuzu.Database(db_path, read_only=True)
            self.conn = kuzu.Connection(self.db)

        @property
        def get_schema_dict(self) -> dict[str, list[dict[str, object]]]:
            response = self.conn.execute(
                "CALL SHOW_TABLES() WHERE type = 'NODE' RETURN *;"
            )
            nodes: list[str] = [row[1] for row in response]  # type: ignore
            response = self.conn.execute(
                "CALL SHOW_TABLES() WHERE type = 'REL' RETURN *;"
            )
            rel_tables: list[str] = [row[1] for row in response]  # type: ignore
            relationships: list[dict[str, str]] = []
            for tbl_name in rel_tables:
                response = self.conn.execute(
                    f"CALL SHOW_CONNECTION('{tbl_name}') RETURN *;"
                )
                for row in response:
                    relationships.append(
                        {"name": tbl_name, "from": row[0], "to": row[1]}
                    )  # type: ignore
            schema: dict[str, list[dict[str, object]]] = {"nodes": [], "edges": []}

            for node in nodes:
                node_schema: dict[str, object] = {"label": node, "properties": []}
                node_properties = self.conn.execute(
                    f"CALL TABLE_INFO('{node}') RETURN *;"
                )
                for row in node_properties:  # type: ignore
                    node_schema["properties"].append(  # type: ignore
                        {"name": row[1], "type": row[2]}
                    )  # type: ignore
                schema["nodes"].append(node_schema)  # type: ignore

            for rel in relationships:
                edge: dict[str, object] = {
                    "label": rel["name"],
                    "from": rel["from"],
                    "to": rel["to"],
                    "properties": [],
                }
                rel_properties = self.conn.execute(
                    f"""CALL TABLE_INFO('{rel["name"]}') RETURN *;"""
                )
                for row in rel_properties:  # type: ignore
                    edge["properties"].append(  # type: ignore
                        {"name": row[1], "type": row[2]}
                    )  # type: ignore
                schema["edges"].append(edge)  # type: ignore
            return schema

    return (KuzuDatabaseManager,)


@app.cell
def __graph_rag_module(
    AnswerQuestion,
    KuzuDatabaseManager,
    PruneSchema,
    Query,
    Text2Cypher,
    dspy,
) -> tuple:
    class GraphRAG(dspy.Module):
        """
        DSPy custom module that applies Text2Cypher to generate a query and run it
        on the Kuzu database, to generate a natural language response.
        """

        def __init__(self) -> None:
            self.prune = dspy.Predict(PruneSchema)
            self.text2cypher = dspy.ChainOfThought(Text2Cypher)
            self.generate_answer = dspy.ChainOfThought(AnswerQuestion)

        def get_cypher_query(self, question: str, input_schema: str) -> Query:  # type: ignore
            prune_result = self.prune(question=question, input_schema=input_schema)
            schema = prune_result.pruned_schema
            text2cypher_result = self.text2cypher(
                question=question, input_schema=schema
            )
            cypher_query: Query = text2cypher_result.query  # type: ignore
            return cypher_query

        def run_query(
            self, db_manager: KuzuDatabaseManager, question: str, input_schema: str  # type: ignore
        ) -> tuple[str, list[object] | None]:
            """
            Run a query synchronously on the database.
            """
            result: Query = self.get_cypher_query(  # type: ignore
                question=question, input_schema=input_schema
            )
            query: str = result.query  # type: ignore
            results: list[object] | None
            try:
                # Run the query on the database
                query_result = db_manager.conn.execute(query)
                results = [item for row in query_result for item in row]
            except RuntimeError as e:
                print(f"Error running query: {e}")
                results = None
            return query, results

        def forward(
            self, db_manager: KuzuDatabaseManager, question: str, input_schema: str  # type: ignore
        ) -> dict[str, object]:
            final_query: str
            final_context: list[object] | None
            final_query, final_context = self.run_query(
                db_manager, question, input_schema
            )
            if final_context is None:
                print(
                    "Empty results obtained from the graph database. Please retry with a different question."
                )
                return {}
            else:
                answer = self.generate_answer(
                    question=question,
                    cypher_query=final_query,
                    context=str(final_context),
                )
                response: dict[str, object] = {
                    "question": question,
                    "query": final_query,
                    "answer": answer,
                }
                return response

        async def aforward(
            self, db_manager: KuzuDatabaseManager, question: str, input_schema: str  # type: ignore
        ) -> dict[str, object]:
            final_query: str
            final_context: list[object] | None
            final_query, final_context = self.run_query(
                db_manager, question, input_schema
            )
            if final_context is None:
                print(
                    "Empty results obtained from the graph database. Please retry with a different question."
                )
                return {}
            else:
                answer = self.generate_answer(
                    question=question,
                    cypher_query=final_query,
                    context=str(final_context),
                )
                response: dict[str, object] = {
                    "question": question,
                    "query": final_query,
                    "answer": answer,
                }
                return response

    def run_graph_rag(
        questions: list[str], db_manager: KuzuDatabaseManager  # type: ignore
    ) -> list[dict[str, object]]:
        schema: str = str(db_manager.get_schema_dict)
        rag: GraphRAG = GraphRAG()
        # Run pipeline
        results: list[dict[str, object]] = []
        for question in questions:
            response: dict[str, object] = rag(
                db_manager=db_manager, question=question, input_schema=schema
            )
            results.append(response)
        return results

    return (run_graph_rag,)


if __name__ == "__main__":
    app.run()
