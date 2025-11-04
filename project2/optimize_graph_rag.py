#!/usr/bin/env python3
"""Optimizer script for Graph RAG using KNNFewShot."""

import argparse
import os
from pathlib import Path

import dspy
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

# Import our modules
from graph_rag_lib import (
    AnswerQuestion,
    KuzuDatabaseManager,
    Text2Cypher,
)
from trainset import get_trainset

# Load environment variables
load_dotenv()


def setup_dspy(model_name: str = "openrouter/google/gemini-2.5-flash"):
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError("OPENROUTER_API_KEY not found in environment variables")

    lm = dspy.LM(
        model=model_name,
        api_base="https://openrouter.ai/api/v1",
        api_key=api_key,
    )
    dspy.configure(lm=lm)
    print(f"Configured DSPy with model: {model_name}")


def create_knn_optimizer(
    k: int = 3, embedder_model: str = "all-MiniLM-L6-v2"
) -> dspy.KNNFewShot:
    print(f"\nLoading embedder model: {embedder_model}")
    embedder = SentenceTransformer(embedder_model)
    print("Embedder loaded successfully")

    trainset = get_trainset()
    print(f"Loaded training set with {len(trainset)} examples")

    knn_optimizer = dspy.KNNFewShot(
        k=k, trainset=trainset, vectorizer=dspy.Embedder(embedder.encode)
    )
    print(f"Created KNNFewShot optimizer (k={k})")

    return knn_optimizer


def optimize_text2cypher(
    optimizer: dspy.KNNFewShot, db_manager: KuzuDatabaseManager
) -> dspy.Module:
    print("\nOptimizing Text2Cypher module...")
    text2cypher_module = dspy.ChainOfThought(Text2Cypher)
    compiled_text2cypher = optimizer.compile(student=text2cypher_module)
    print("Text2Cypher module optimized")
    return compiled_text2cypher


def optimize_answer_generator(optimizer: dspy.KNNFewShot) -> dspy.Module:
    print("\nOptimizing AnswerQuestion module...")
    answer_module = dspy.ChainOfThought(AnswerQuestion)
    compiled_answer = optimizer.compile(student=answer_module)
    print("AnswerQuestion module optimized")
    return compiled_answer


def save_optimized_modules(
    text2cypher_module: dspy.Module,
    answer_module: dspy.Module,
    output_dir: str = ".",
):
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    text2cypher_path = output_path / "compiled_text2cypher.json"
    answer_path = output_path / "compiled_answer.json"

    print(f"\nSaving optimized modules to {output_path}")

    text2cypher_module.save(str(text2cypher_path))
    print(f"Saved Text2Cypher to: {text2cypher_path}")

    answer_module.save(str(answer_path))
    print(f"Saved AnswerQuestion to: {answer_path}")


def test_optimized_modules(
    text2cypher_module: dspy.Module, db_manager: KuzuDatabaseManager
):
    print("\nTesting optimized modules...")
    print("Note: This test makes LLM API calls and may be slow (30-60s)")

    schema = str(db_manager.get_schema_dict)
    test_question = "Which scholars won prizes in Physics?"

    print(f"Test question: {test_question}")

    try:
        result = text2cypher_module(question=test_question, input_schema=schema)
        query = result.query.query if hasattr(result, "query") else str(result)

        print(f"Generated query: {query}")

        db_manager.conn.execute(f"EXPLAIN {query}")
        print("Query is syntactically valid")

    except Exception as e:
        print(f"Test failed: {e}")
        print("The modules are still saved and can be used")


def main():
    parser = argparse.ArgumentParser(description="Optimize Graph RAG with KNNFewShot")
    parser.add_argument(
        "--k",
        type=int,
        default=3,
        help="Number of nearest neighbors to retrieve (default: 3)",
    )
    parser.add_argument(
        "--embedder",
        type=str,
        default="all-MiniLM-L6-v2",
        help="SentenceTransformer model name (default: all-MiniLM-L6-v2)",
    )
    parser.add_argument(
        "--db",
        type=str,
        default="nobel.kuzu",
        help="Path to Kuzu database (default: nobel.kuzu)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=".",
        help="Directory to save optimized modules (default: current directory)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run test after compilation (may be slow due to LLM API calls)",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Graph RAG Optimization with KNNFewShot")
    print("=" * 60)

    setup_dspy()

    print(f"\nConnecting to database: {args.db}")
    db_manager = KuzuDatabaseManager(args.db)
    print("Database connection established")

    optimizer = create_knn_optimizer(k=args.k, embedder_model=args.embedder)

    compiled_text2cypher = optimize_text2cypher(optimizer, db_manager)
    compiled_answer = optimize_answer_generator(optimizer)

    save_optimized_modules(
        compiled_text2cypher, compiled_answer, output_dir=args.output_dir
    )

    if args.test:
        test_optimized_modules(compiled_text2cypher, db_manager)
    else:
        print("\nSkipped testing (use --test flag to test compiled modules)")

    print("\n" + "=" * 60)
    print("Optimization complete!")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. Run evaluate_optimizer.py to benchmark performance")
    print("  2. Use the compiled modules in graph_rag.py")
    print(f"  3. Compiled modules saved in: {Path(args.output_dir).absolute()}")


if __name__ == "__main__":
    main()
