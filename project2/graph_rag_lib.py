"""
Graph RAG Library - Reusable components for Text2Cypher and Graph RAG.
"""

import hashlib
from typing import Any

import dspy
import kuzu
from cachetools import LRUCache
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


class QuestionRelevance(dspy.Signature):
    """
    Check if a question is about Nobel Prizes. Be very permissive - only reject obviously unrelated questions.

    Return True for ANY question that mentions:
    - Nobel Prize, Nobel, laureate, winner
    - Physics, Chemistry, Literature, Peace, Economics (prize categories)
    - Scientists, scholars, researchers, academics
    - Universities, institutions, countries, affiliations
    - Years, awards, prizes

    Return False ONLY for clearly unrelated questions like:
    - General knowledge questions not related to Nobel Prizes or laureates
    - Personal questions about the user

    When in doubt, return True. It's better to try answering than to reject.
    """

    question: str = dspy.InputField()
    is_relevant: bool = dspy.OutputField(
        description="True unless question is clearly unrelated to Nobel Prize information"
    )


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
    - NEVER return hardcoded strings, messages, or literals using RETURN 'some message' syntax.
    - ALWAYS query actual data from the database nodes and relationships.
    - If you cannot construct a valid query, the system will handle the error appropriately.
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

    # Note: Caches are instance-scoped to avoid cross-configuration warm-cache effects in benchmarks

    def validate_cypher_query(
        self, _args: dict[Any, Any], pred: dspy.Prediction
    ) -> float:
        """Reward function for Refine; also counts validation attempts.

        Side effect: increments self._validation_calls so we can later report
        how many refinement iterations were used for a query. This enables
        benchmarking of the self-refinement loop effectiveness.
        """
        # Track number of times validation was invoked for the current query
        if hasattr(self, "_validation_calls"):
            self._validation_calls += 1
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

            # Check for hardcoded string returns (common escape pattern)
            q_lower = q.lower()
            if "return '" in q_lower or 'return "' in q_lower:
                # Check if it's returning a hardcoded string message
                if any(
                    phrase in q_lower
                    for phrase in [
                        "cannot answer",
                        "don't have",
                        "not available",
                        "philosophical",
                        "outside",
                        "beyond",
                    ]
                ):
                    print(f"Invalid Cypher query (hardcoded message escape): {q}")
                    return 0.0

            # Validate syntax with EXPLAIN
            dbm.conn.execute(f"EXPLAIN {q}")

            if "return" not in q_lower:
                print(f"Invalid Cypher query (no RETURN): {q}")
                return 0.0

            print("Valid Cypher query:", q)
            return 1.0
        except Exception as e:
            print(f"Invalid Cypher query: {q} (Error: {e})")
            return 0.0

    def __init__(
        self,
        use_knn_fewshot: bool = False,
        use_validation: bool = True,
        k: int = 3,
        knn_static: bool = False,
        use_postprocessing: bool = False,
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
        self.knn_static = knn_static
        self.use_postprocessing = use_postprocessing
        # Counter used during validation to record refinement iterations
        self._validation_calls = 0
        # Instance-level caches
        self._prune_cache: LRUCache = LRUCache(maxsize=128)
        self._text2cypher_cache: LRUCache = LRUCache(maxsize=128)

        # Stage 0: Question relevance validation
        self.validate_relevance = dspy.Predict(QuestionRelevance)

        # Stage 1: Schema pruning
        self.prune = dspy.Predict(PruneSchema)

        # Stage 2: Query generation (with optional KNN)
        if use_knn_fewshot:
            self._setup_knn(k, static=self.knn_static)
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
            fewshot_mode = "static" if self.knn_static else "KNN"
            config_parts.append(f"{fewshot_mode} few-shot (k={k})")
        if use_validation:
            config_parts.append(
                f"Refine self-refinement (max {self.max_retries} iterations)"
            )
        if use_postprocessing:
            config_parts.append("Post-processing")
        if not config_parts:
            config_parts.append("baseline")

        print(f"GraphRAG agent: {' + '.join(config_parts)}")

    def clear_caches(self) -> None:
        """Clear instance caches for schema pruning and text-to-Cypher."""
        self._prune_cache.clear()
        self._text2cypher_cache.clear()

    def _setup_knn(self, k: int, static: bool = False):
        """Set up few-shot optimizer for query generation.

        If static=True, we simulate a static few-shot prompt by using a constant
        vectorizer so that the first k examples are always selected deterministically.
        """
        from sentence_transformers import SentenceTransformer
        from trainset import get_trainset

        trainset = get_trainset()

        if static:
            print(f"Loading STATIC few-shot with k={k} (deterministic selection)...")

            class _ConstVectorizer:
                def __call__(self, texts):
                    # Return zero vectors of fixed size; KNN will pick first k
                    if isinstance(texts, str):
                        texts = [texts]
                    import numpy as np

                    return np.zeros((len(texts), 384), dtype=float)

            knn_optimizer = dspy.KNNFewShot(
                k=k, trainset=trainset, vectorizer=_ConstVectorizer()
            )
        else:
            print(f"Loading KNN optimizer with k={k}...")
            embedder = SentenceTransformer("all-MiniLM-L6-v2")
            knn_optimizer = dspy.KNNFewShot(
                k=k, trainset=trainset, vectorizer=dspy.Embedder(embedder.encode)
            )

        base_module = dspy.ChainOfThought(Text2Cypher)
        self.text2cypher = knn_optimizer.compile(student=base_module)
        print(f"Few-shot ready with {len(trainset)} examples")

    def _post_process_query(self, q: str) -> str:
        """Apply light, safe post-processing to improve query robustness.

        - Trim whitespace and semicolons
        - Collapse newlines to spaces
        - Ensure single space between tokens
        - Leave semantics unchanged (no risky rewrites)
        """
        if not q or not isinstance(q, str):
            return q
        import re

        q2 = q.strip().rstrip(";")
        q2 = re.sub(r"\s+", " ", q2)
        return q2

    def _make_cache_key(self, question: str, schema: str) -> str:
        q_hash = hashlib.md5(question.encode()).hexdigest()[:8]
        s_hash = hashlib.md5(str(schema).encode()).hexdigest()[:8]
        return f"{q_hash}_{s_hash}"

    def forward(
        self, db_manager: KuzuDatabaseManager, question: str, input_schema: str
    ):
        """
        Multi-step agent execution pipeline.

        Stages:
        0. Validate question relevance to domain
        1. Prune schema to relevant elements
        2. Generate Cypher query (with optional KNN examples and Refine)
        3. Execute query on database
        4. Generate natural language answer
        """
        import time

        self._db_manager = db_manager
        timing_info = {}

        # Stage 0: Question Relevance Validation
        relevance_start = time.time()
        print(f"Starting relevance check for: {question[:50]}...")

        # Add debug info to see what DSPy is actually doing
        import dspy

        print(f"Current LM: {dspy.settings.lm}")

        relevance_result = self.validate_relevance(question=question)
        relevance_time = time.time() - relevance_start
        timing_info["relevance_check"] = relevance_time

        print(
            f"Relevance check completed in {relevance_time:.3f}s, result: {relevance_result.is_relevant}"
        )
        print(f"DSPy prediction object: {type(relevance_result)}")

        if not relevance_result.is_relevant:
            print("Question not relevant to Nobel Prize domain")
            irrelevant_answer = dspy.Prediction(
                response="I can only answer questions about Nobel Prize laureates, prizes, and related information."
            )
            return {
                "question": question,
                "query": "N/A - Question not relevant to domain",
                "answer": irrelevant_answer,
                "timing_info": timing_info,
            }

        # Stage 1: Schema Pruning (with caching)
        prune_start = time.time()
        prune_key = self._make_cache_key(question, input_schema)
        if prune_key in self._prune_cache:
            print(f"Cache hit: pruning (key={prune_key})")
            schema = self._prune_cache[prune_key]
            cache_hit_prune = True
        else:
            print(f"Cache miss: pruning (key={prune_key})")
            prune_result = self.prune(question=question, input_schema=input_schema)
            schema = prune_result.pruned_schema
            self._prune_cache[prune_key] = schema
            cache_hit_prune = False
        timing_info["schema_pruning"] = time.time() - prune_start
        timing_info["cache_hit_prune"] = cache_hit_prune

        # Stage 2: Query Generation (with optional KNN and Refine, with caching)
        query_start = time.time()
        text2cypher_key = self._make_cache_key(question, str(schema))
        # Reset validation counter for this new query attempt
        self._validation_calls = 0
        if text2cypher_key in self._text2cypher_cache:
            print(f"Cache hit: text2cypher (key={text2cypher_key})")
            query_string = self._text2cypher_cache[text2cypher_key]
            cache_hit_query = True
            refine_iterations = 0  # Cached result implies no refinement this call
        else:
            print(f"Cache miss: text2cypher (key={text2cypher_key})")
            result = self.text2cypher(question=question, input_schema=schema)
            query_string = result.query.query
            self._text2cypher_cache[text2cypher_key] = query_string
            cache_hit_query = False
            # If validation was enabled, number of refinement iterations equals validation calls
            refine_iterations = self._validation_calls if self.use_validation else 0
        timing_info["query_generation"] = time.time() - query_start
        timing_info["cache_hit_query"] = cache_hit_query
        timing_info["refine_iterations"] = refine_iterations

        # Optional post-processing stage
        if getattr(self, "use_postprocessing", False):
            pp_start = time.time()
            original_query = query_string
            query_string = self._post_process_query(query_string)
            timing_info["post_processing"] = time.time() - pp_start
            if original_query != query_string:
                print("Applied post-processing to query")

        # Stage 3: Execute Query
        exec_start = time.time()
        try:
            query_result = db_manager.conn.execute(query_string)
            context = [item for row in query_result for item in row]
            timing_info["result_count"] = len(context)
        except Exception as e:
            print(f"Error executing query: {e}")
            timing_info["query_execution"] = time.time() - exec_start
            timing_info["answer_generation"] = 0
            error_answer = dspy.Prediction(
                response="I don't have enough information to answer this question."
            )
            return {
                "question": question,
                "query": query_string,
                "answer": error_answer,
                "timing_info": timing_info,
                "query_length": len(query_string.split()),
                "context": [],
            }
        timing_info["query_execution"] = time.time() - exec_start
        timing_info["query_length"] = len(query_string.split())

        # Stage 4: Generate Answer
        if not context:
            print("Empty results from database")
            timing_info["answer_generation"] = 0
            empty_answer = dspy.Prediction(
                response="No results were found for this query."
            )
            return {
                "question": question,
                "query": query_string,
                "answer": empty_answer,
                "timing_info": timing_info,
                "query_length": len(query_string.split()),
                "context": [],
            }

        answer_start = time.time()
        answer = self.generate_answer(
            question=question,
            cypher_query=query_string,
            context=str(context),
        )
        timing_info["answer_generation"] = time.time() - answer_start

        return {
            "question": question,
            "query": query_string,
            "answer": answer,
            "timing_info": timing_info,
            "query_length": len(query_string.split()),
            "context": context,
        }


def create_graph_rag(
    use_knn: bool = False,
    use_validation: bool = True,
    k: int = 3,
    knn_static: bool = False,
    use_postprocessing: bool = False,
) -> GraphRAG:
    return GraphRAG(
        use_knn_fewshot=use_knn,
        use_validation=use_validation,
        k=k,
        knn_static=knn_static,
        use_postprocessing=use_postprocessing,
    )


def run_graph_rag(
    questions: list[str],
    db_manager: KuzuDatabaseManager,
    rag_instance: GraphRAG | None = None,
    use_knn: bool = False,
    use_validation: bool = True,
    k: int = 3,
    knn_static: bool = False,
    use_postprocessing: bool = False,
) -> list[Any]:
    schema = str(db_manager.get_schema_dict)

    if rag_instance is None:
        print(
            f"Creating GraphRAG (knn={use_knn}, validation={use_validation}, k={k}, static={knn_static}, postproc={use_postprocessing})"
        )
        rag = GraphRAG(
            use_knn_fewshot=use_knn,
            use_validation=use_validation,
            k=k,
            knn_static=knn_static,
            use_postprocessing=use_postprocessing,
        )
    else:
        rag = rag_instance

    results = []
    for question in questions:
        print(f"question: {question}")
        response = rag(db_manager=db_manager, question=question, input_schema=schema)
        results.append(response)
    return results
