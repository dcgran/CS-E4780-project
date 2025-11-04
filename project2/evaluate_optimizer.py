#!/usr/bin/env python3
"""Evaluation script for comparing baseline vs KNN-optimized Graph RAG."""

import argparse
import json
import time
from pathlib import Path
from typing import Any

import dspy
from dotenv import load_dotenv
import os

from graph_rag_lib import KuzuDatabaseManager, create_graph_rag, run_graph_rag
from trainset import get_validation_set

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


def validate_cypher_syntax(query: str, db_manager: KuzuDatabaseManager) -> bool:
    try:
        db_manager.conn.execute(f"EXPLAIN {query}")
        return True
    except Exception:
        return False


def evaluate_single_question(
    question: str,
    db_manager: KuzuDatabaseManager,
    rag_instance,
) -> dict[str, Any]:
    start_time = time.time()

    try:
        result = run_graph_rag([question], db_manager, rag_instance=rag_instance)[0]

        latency = time.time() - start_time

        query = result.get("query", "")
        answer = result.get("answer", {})
        answer_text = answer.response if hasattr(answer, "response") else str(answer)

        is_valid = validate_cypher_syntax(query, db_manager)

        return {
            "question": question,
            "query": query,
            "answer": answer_text,
            "latency": latency,
            "query_valid": is_valid,
            "success": True,
        }

    except Exception as e:
        latency = time.time() - start_time
        return {
            "question": question,
            "error": str(e),
            "latency": latency,
            "query_valid": False,
            "success": False,
        }


def run_benchmark(
    db_manager: KuzuDatabaseManager,
    test_questions: list[str],
    use_knn: bool = False,
    k: int = 3,
) -> dict[str, Any]:
    mode = "KNN Few-Shot" if use_knn else "Baseline (Zero-Shot)"
    print(f"\n{'=' * 60}")
    print(f"Running benchmark: {mode}")
    print(f"{'=' * 60}")

    print(f"\nInitializing GraphRAG (use_knn={use_knn}, k={k})...")
    init_start = time.time()
    rag_instance = create_graph_rag(use_knn=use_knn, k=k)
    init_time = time.time() - init_start
    print(f"Initialization complete ({init_time:.2f}s)")

    results = []
    for i, question in enumerate(test_questions, 1):
        print(f"\n[{i}/{len(test_questions)}] Testing: {question[:60]}...")

        result = evaluate_single_question(question, db_manager, rag_instance)

        results.append(result)

        if result["success"]:
            status = "Valid" if result["query_valid"] else "Invalid"
            print(f"  {status} query | Latency: {result['latency']:.2f}s")
        else:
            print(f"  Error: {result.get('error', 'Unknown')}")

    total_questions = len(results)
    successful = sum(1 for r in results if r["success"])
    valid_queries = sum(1 for r in results if r.get("query_valid", False))
    avg_latency = sum(r["latency"] for r in results) / total_questions

    metrics = {
        "mode": mode,
        "total_questions": total_questions,
        "successful": successful,
        "valid_queries": valid_queries,
        "success_rate": successful / total_questions,
        "query_validity_rate": valid_queries / total_questions,
        "avg_latency": avg_latency,
        "results": results,
    }

    return metrics


def print_comparison(baseline_metrics: dict, knn_metrics: dict):
    print("\n" + "=" * 60)
    print("PERFORMANCE COMPARISON")
    print("=" * 60)
    print(f"\n{'Metric':<30} {'Baseline':<15} {'KNN Few-Shot':<15} {'Improvement'}")
    print("-" * 75)

    baseline_success = baseline_metrics["success_rate"]
    knn_success = knn_metrics["success_rate"]
    success_improvement = (
        ((knn_success - baseline_success) / baseline_success * 100)
        if baseline_success > 0
        else 0
    )
    print(
        f"{'Success Rate':<30} {baseline_success:<15.2%} {knn_success:<15.2%} {success_improvement:+.1f}%"
    )

    baseline_validity = baseline_metrics["query_validity_rate"]
    knn_validity = knn_metrics["query_validity_rate"]
    validity_improvement = (
        ((knn_validity - baseline_validity) / baseline_validity * 100)
        if baseline_validity > 0
        else 0
    )
    print(
        f"{'Query Validity Rate':<30} {baseline_validity:<15.2%} {knn_validity:<15.2%} {validity_improvement:+.1f}%"
    )

    baseline_latency = baseline_metrics["avg_latency"]
    knn_latency = knn_metrics["avg_latency"]
    latency_change = (
        ((knn_latency - baseline_latency) / baseline_latency * 100)
        if baseline_latency > 0
        else 0
    )
    print(
        f"{'Average Latency (s)':<30} {baseline_latency:<15.2f} {knn_latency:<15.2f} {latency_change:+.1f}%"
    )

    print("-" * 75)

    print("\nSummary:")
    if knn_validity > baseline_validity:
        print(f"  KNN few-shot improved query validity by {validity_improvement:.1f}%")
    elif knn_validity < baseline_validity:
        print(
            f"  KNN few-shot decreased query validity by {abs(validity_improvement):.1f}%"
        )
    else:
        print("  Query validity remained the same")

    if knn_latency < baseline_latency:
        print(f"  KNN few-shot reduced latency by {abs(latency_change):.1f}%")
    elif knn_latency > baseline_latency:
        print(f"  KNN few-shot increased latency by {latency_change:.1f}%")
    else:
        print("  Latency remained the same")


def save_results(baseline_metrics: dict, knn_metrics: dict, output_file: str):
    output = {
        "baseline": baseline_metrics,
        "knn_few_shot": knn_metrics,
        "comparison": {
            "success_rate_improvement": knn_metrics["success_rate"]
            - baseline_metrics["success_rate"],
            "validity_rate_improvement": knn_metrics["query_validity_rate"]
            - baseline_metrics["query_validity_rate"],
            "latency_change": knn_metrics["avg_latency"]
            - baseline_metrics["avg_latency"],
        },
    }

    output_path = Path(output_file)
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nDetailed results saved to: {output_path.absolute()}")


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate baseline vs KNN-optimized Graph RAG"
    )
    parser.add_argument(
        "--db",
        type=str,
        default="nobel.kuzu",
        help="Path to Kuzu database (default: nobel.kuzu)",
    )
    parser.add_argument(
        "--k",
        type=int,
        default=3,
        help="Number of nearest neighbors for KNN (default: 3)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="evaluation_results.json",
        help="Output file for detailed results (default: evaluation_results.json)",
    )
    parser.add_argument(
        "--use-validation-set",
        action="store_true",
        help="Use predefined validation set instead of custom questions",
    )
    parser.add_argument(
        "--custom-questions",
        nargs="+",
        help="Custom questions to test (space-separated)",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Graph RAG Evaluation: Baseline vs KNN Few-Shot")
    print("=" * 60)

    setup_dspy()

    print(f"\nConnecting to database: {args.db}")
    db_manager = KuzuDatabaseManager(args.db)
    print("Database connection established")

    if args.use_validation_set:
        validation_set = get_validation_set()
        test_questions = [example.question for example in validation_set]
        print(f"\nUsing validation set ({len(test_questions)} questions)")
    elif args.custom_questions:
        test_questions = args.custom_questions
        print(f"\nUsing custom questions ({len(test_questions)} questions)")
    else:
        test_questions = [
            "Which scholars won prizes in Physics?",
            "Which scholars were affiliated with University of Cambridge?",
            "Which scholars won prizes in Chemistry and were affiliated with MIT?",
            "How many scholars won prizes in Literature?",
            "What prizes did Marie Curie win?",
        ]
        print(f"\nUsing default test questions ({len(test_questions)} questions)")

    baseline_metrics = run_benchmark(db_manager, test_questions, use_knn=False)

    knn_metrics = run_benchmark(db_manager, test_questions, use_knn=True, k=args.k)

    print_comparison(baseline_metrics, knn_metrics)

    save_results(baseline_metrics, knn_metrics, args.output)

    print("\n" + "=" * 60)
    print("Evaluation complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
