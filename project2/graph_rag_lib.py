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
    Multi-step agent for Graph RAG with optional KNN few-shot and validation.

    Pipeline stages:
    1. Schema pruning (select relevant graph elements)
    2. Query generation (with optional KNN examples)
    3. Validation loop (syntax check with EXPLAIN)
    4. Query execution
    5. Answer generation
    """

    def validate_cypher_query(self, _args: dict[Any, Any], pred: dspy.Prediction) -> float:
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

    def __init__(
        self, use_knn_fewshot: bool = False, use_validation: bool = True, k: int = 3
    ):
        """
        Initialize multi-step GraphRAG agent.

        Args:
            use_knn_fewshot: If True, use KNN to retrieve similar examples
            use_validation: If True, use Refine for self-refinement loop
            k: Number of nearest neighbors for KNN
        """
        super().__init__()

        # Configuration
        self.use_knn = use_knn_fewshot
        self.use_validation = use_validation
        self.max_retries = 3

        # Stage 1: Schema pruning
        self.prune = dspy.Predict(PruneSchema)

        # Stage 2: Query generation (with optional KNN)
        if use_knn_fewshot:
            self._setup_knn(k)
        else:
            self.text2cypher = dspy.ChainOfThought(Text2Cypher)

        # Wrap with Refine for self-refinement if validation enabled
        if use_validation:
            self.text2cypher = dspy.Refine(
                self.text2cypher,
                N=self.max_retries,
                reward_fn=self.validate_cypher_query,
                threshold=1.0,
            )

        # Stage 5: Answer generation
        self.generate_answer = dspy.ChainOfThought(AnswerQuestion)

        config_parts: list[str] = []
        if use_knn_fewshot:
            config_parts.append(f"KNN few-shot (k={k})")
        if use_validation:
            config_parts.append(f"Refine self-refinement (max {self.max_retries} iterations)")
        if not config_parts:
            config_parts.append("baseline")

        print(f"GraphRAG agent: {' + '.join(config_parts)}")

    def _setup_knn(self, k: int):
        """Set up KNN few-shot optimizer for query generation."""
        from sentence_transformers import SentenceTransformer
        from trainset import get_trainset

        print(f"Loading KNN optimizer with k={k}...")
        embedder = SentenceTransformer("all-MiniLM-L6-v2")
        trainset = get_trainset()

        knn_optimizer = dspy.KNNFewShot(
            k=k, trainset=trainset, vectorizer=dspy.Embedder(embedder.encode)
        )

        base_module = dspy.ChainOfThought(Text2Cypher)
        self.text2cypher = knn_optimizer.compile(student=base_module)
        print(f"KNN ready with {len(trainset)} examples")

    def forward(
        self, db_manager: KuzuDatabaseManager, question: str, input_schema: str
    ):
        """
        Multi-step agent execution pipeline.

        Stages:
        1. Prune schema to relevant elements
        2. Generate Cypher query (with optional KNN examples and Refine)
        3. Execute query on database
        4. Generate natural language answer
        """
        self._db_manager = db_manager

        # Stage 1: Schema Pruning
        prune_result = self.prune(question=question, input_schema=input_schema)
        schema = prune_result.pruned_schema

        # Stage 2: Query Generation (with optional KNN and Refine)
        result = self.text2cypher(question=question, input_schema=schema)
        query_string = result.query.query

        # Stage 3: Execute Query
        try:
            query_result = db_manager.conn.execute(query_string)
            context = [item for row in query_result for item in row]
        except Exception as e:
            print(f"Error executing query: {e}")
            error_answer = dspy.Prediction(
                response="I don't have enough information to answer this question."
            )
            return {
                "question": question,
                "query": query_string,
                "answer": error_answer,
            }

        # Stage 4: Generate Answer
        if not context:
            print("Empty results from database")
            empty_answer = dspy.Prediction(
                response="No results were found for this query."
            )
            return {
                "question": question,
                "query": query_string,
                "answer": empty_answer,
            }

        answer = self.generate_answer(
            question=question,
            cypher_query=query_string,
            context=str(context),
        )

        return {
            "question": question,
            "query": query_string,
            "answer": answer,
        }


def create_graph_rag(
    use_knn: bool = False, use_validation: bool = True, k: int = 3
) -> GraphRAG:
    return GraphRAG(use_knn_fewshot=use_knn, use_validation=use_validation, k=k)


def run_graph_rag(
    questions: list[str],
    db_manager: KuzuDatabaseManager,
    rag_instance: GraphRAG | None = None,
    use_knn: bool = False,
    use_validation: bool = True,
    k: int = 3,
) -> list[Any]:
    schema = str(db_manager.get_schema_dict)

    if rag_instance is None:
        print(f"Creating GraphRAG (knn={use_knn}, validation={use_validation}, k={k})")
        rag = GraphRAG(
            use_knn_fewshot=use_knn, use_validation=use_validation, k=k
        )
    else:
        rag = rag_instance

    results = []
    for question in questions:
        print(f"question: {question}")
        response = rag(db_manager=db_manager, question=question, input_schema=schema)
        results.append(response)
    return results
