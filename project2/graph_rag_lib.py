"""
Graph RAG Library - Reusable components for Text2Cypher and Graph RAG.
"""

from typing import Any

import dspy
import kuzu
from pydantic import BaseModel, Field


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
    pruned_schema: GraphSchema = dspy.OutputField()


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
    query: Query = dspy.OutputField()


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


class KuzuDatabaseManager:
    """Manages Kuzu database connection and schema retrieval."""

    def __init__(self, db_path: str = "ldbc_1.kuzu"):
        self.db_path = db_path
        self.db = kuzu.Database(db_path, read_only=True)
        self.conn = kuzu.Connection(self.db)

    @property
    def get_schema_dict(self) -> dict[str, list[dict]]:
        response = self.conn.execute("CALL SHOW_TABLES() WHERE type = 'NODE' RETURN *;")
        nodes = [row[1] for row in response]  # type: ignore
        response = self.conn.execute("CALL SHOW_TABLES() WHERE type = 'REL' RETURN *;")
        rel_tables = [row[1] for row in response]  # type: ignore
        relationships = []
        for tbl_name in rel_tables:
            response = self.conn.execute(
                f"CALL SHOW_CONNECTION('{tbl_name}') RETURN *;"
            )
            for row in response:
                relationships.append({"name": tbl_name, "from": row[0], "to": row[1]})  # type: ignore
        schema: dict[str, list[dict]] = {"nodes": [], "edges": []}

        for node in nodes:
            node_schema = {"label": node, "properties": []}
            node_properties = self.conn.execute(f"CALL TABLE_INFO('{node}') RETURN *;")
            for row in node_properties:  # type: ignore
                node_schema["properties"].append({"name": row[1], "type": row[2]})  # type: ignore
            schema["nodes"].append(node_schema)

        for rel in relationships:
            edge = {
                "label": rel["name"],
                "from": rel["from"],
                "to": rel["to"],
                "properties": [],
            }
            rel_properties = self.conn.execute(
                f"""CALL TABLE_INFO('{rel["name"]}') RETURN *;"""
            )
            for row in rel_properties:  # type: ignore
                edge["properties"].append({"name": row[1], "type": row[2]})  # type: ignore
            schema["edges"].append(edge)
        return schema


class GraphRAG(dspy.Module):
    """
    DSPy custom module that applies Text2Cypher to generate a query and run it
    on the Kuzu database, to generate a natural language response.
    """

    def validate_cypher_query(self, args, pred: dspy.Prediction) -> float:
        try:
            q = getattr(getattr(pred, "query", None), "query", None)
            if not q or not isinstance(q, str):
                return 0.0
            q = q.strip()
            if not q:
                return 0.0

            dbm = getattr(self, "_db_manager", None)
            if dbm is None or not hasattr(dbm, "conn"):
                print("No database manager available for query validation.")
                return 0.0

            dbm.conn.execute(f"EXPLAIN {q}")

            q_lower = q.lower()
            if "return" not in q_lower:
                print(f"Invalid Cypher query (no RETURN): {q}")
                return 0.0

            print("Valid Cypher query:", q)
            return 1.0
        except Exception as e:
            print(f"Invalid Cypher query: {q} (Error: {e})")
            return 0.0

    def __init__(self, use_knn_fewshot: bool = False, k: int = 3):
        """
        Initialize GraphRAG module.

        Args:
            use_knn_fewshot: If True, use KNN few-shot learning
            k: Number of nearest neighbors for KNN (default: 3)
        """
        self.prune = dspy.Predict(PruneSchema)

        if use_knn_fewshot:
            from sentence_transformers import SentenceTransformer
            from trainset import get_trainset

            print(f"Initializing KNN few-shot with k={k}...")
            embedder = SentenceTransformer("all-MiniLM-L6-v2")
            trainset = get_trainset()

            knn_optimizer = dspy.KNNFewShot(
                k=k, trainset=trainset, vectorizer=dspy.Embedder(embedder.encode)
            )

            # Apply KNN only to Text2Cypher where exemplars help with Cypher syntax
            self.text2cypher = knn_optimizer.compile(
                student=dspy.ChainOfThought(Text2Cypher)
            )
            self.generate_answer = dspy.ChainOfThought(AnswerQuestion)
            print(f"KNN few-shot initialized with {len(trainset)} examples")
            print("Applied to Text2Cypher (query generation)")
            print("AnswerQuestion uses standard ChainOfThought")
        else:
            self.text2cypher = dspy.BestOfN(
                module=dspy.ChainOfThought(Text2Cypher),
                N=3,
                reward_fn=self.validate_cypher_query,
                threshold=1.0,
            )
            self.generate_answer = dspy.ChainOfThought(AnswerQuestion)

    def get_cypher_query(self, question: str, input_schema: str) -> Query:
        prune_result = self.prune(question=question, input_schema=input_schema)
        schema = prune_result.pruned_schema
        text2cypher_result = self.text2cypher(question=question, input_schema=schema)
        cypher_query = text2cypher_result.query

        dbm = getattr(self, "_db_manager", None)
        if dbm is None or not hasattr(dbm, "conn"):
            raise RuntimeError("Database manager not available for query validation.")

        try:
            query_string = (
                cypher_query.query
                if hasattr(cypher_query, "query")
                else str(cypher_query)
            )
            dbm.conn.execute(f"EXPLAIN {query_string}")
        except Exception as e:
            print(f"Query validation failed: {e}")
            print(f"Query was: {query_string}")
            raise RuntimeError(f"Failed to generate a valid Cypher query: {e}")

        return cypher_query

    def run_query(
        self, db_manager: KuzuDatabaseManager, question: str, input_schema: str
    ) -> tuple[str, list[Any] | None]:
        result = self.get_cypher_query(question=question, input_schema=input_schema)
        query = result.query
        try:
            query_result = db_manager.conn.execute(query)
            results = [item for row in query_result for item in row]
        except RuntimeError as e:
            print(f"Error running query: {e}")
            results = None
        return query, results

    def forward(
        self, db_manager: KuzuDatabaseManager, question: str, input_schema: str
    ):
        self._db_manager = db_manager
        final_query, final_context = self.run_query(db_manager, question, input_schema)
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
            response = {
                "question": question,
                "query": final_query,
                "answer": answer,
            }
            return response

    async def aforward(
        self, db_manager: KuzuDatabaseManager, question: str, input_schema: str
    ):
        self._db_manager = db_manager
        final_query, final_context = self.run_query(db_manager, question, input_schema)
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
            response = {
                "question": question,
                "query": final_query,
                "answer": answer,
            }
            return response


def create_graph_rag(use_knn: bool = False, k: int = 3) -> GraphRAG:
    return GraphRAG(use_knn_fewshot=use_knn, k=k)


def run_graph_rag(
    questions: list[str],
    db_manager: KuzuDatabaseManager,
    rag_instance: GraphRAG | None = None,
    use_knn: bool = False,
    k: int = 3,
) -> list[Any]:
    schema = str(db_manager.get_schema_dict)

    if rag_instance is None:
        print(f"Creating GraphRAG (use_knn={use_knn}, k={k})")
        rag = GraphRAG(use_knn_fewshot=use_knn, k=k)
    else:
        rag = rag_instance

    results = []
    for question in questions:
        print(f"question: {question}")
        response = rag(db_manager=db_manager, question=question, input_schema=schema)
        results.append(response)
    return results
