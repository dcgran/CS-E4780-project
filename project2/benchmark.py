"""
Simple benchmark sui        self.test_questions = [
            "Who won the Nobel Prize in Physics in 2020?",
            "Which scholars won prizes in Physics and were affiliated with University of Cambridge?",
            "How many Nobel Prizes have been awarded in Chemistry?",
            "List all Nobel Prize winners from Sweden.",
            "Find all scholars whose name contains 'Einstein'.",
            "What is the weather like today?",  # Should be filtered out
            "Who won the Nobel Prize in Physics in 2020?",  # Duplicate for cache testing
        ]raph RAG performance testing.
"""

import time
import json
import statistics
import os
from typing import Any

import dspy
from dotenv import load_dotenv

from graph_rag_lib import GraphRAG, KuzuDatabaseManager
import re


class GraphRAGBenchmark:
    """Simple benchmark runner for Graph RAG."""

    def __init__(self, db_path: str = "nobel.kuzu"):
        # Configure LM for DSPy
        self._setup_lm()

        self.db_manager = KuzuDatabaseManager(db_path)
        self.test_questions = [
            "Who won the Nobel Prize in Physics in 2020?",
            "Which scholars won prizes in Physics and were affiliated with University of Cambridge?",
            "How many Nobel Prizes have been awarded in Chemistry?",
            "List all Nobel Prize winners from Sweden.",
            "Find all scholars whose name contains 'Einstein'.",
            "Who won the Nobel Prize in Physics in 2020?",  # Duplicate to test cache
        ]

    def _setup_lm(self):
        """Configure the language model for DSPy."""
        load_dotenv()

        api_key = os.environ.get("OPENROUTER_API_KEY")
        if not api_key:
            print("Error: OPENROUTER_API_KEY not found in .env file")
            print("Please create a .env file with: OPENROUTER_API_KEY=your_api_key")
            raise ValueError("Missing OPENROUTER_API_KEY")

        try:
            # Using OpenRouter with Gemini (same as in graph_rag.py)
            lm = dspy.LM(
                model="openrouter/google/gemini-2.5-flash",
                api_base="https://openrouter.ai/api/v1",
                api_key=api_key,
            )
            dspy.configure(lm=lm)
            print("LM configured successfully")
        except Exception as e:
            print(f"Error configuring LM: {e}")
            raise

    def _assess_semantic(
        self, question: str, query: str, timing_info: dict, context: list[Any] | None
    ) -> bool:
        """Heuristic semantic correctness check.

        Returns True if:
          - Query executed successfully (result_count present), and
          - For queries that mention concrete constraints (year, category, name, location),
            those tokens are present in the Cypher query (lowercased string match).
        This is a light heuristic since we don't have labeled ground truth.
        """
        # Must have executed
        if "result_count" not in timing_info:
            return False

        q_lower = query.lower()
        text = question.lower()

        # Check year presence if question mentions a year
        year_match = re.search(r"\b(19|20)\d{2}\b", text)
        if year_match and year_match.group(0) not in q_lower:
            return False

        # Check known categories
        for cat in [
            "physics",
            "chemistry",
            "literature",
            "peace",
            "economics",
            "medicine",
        ]:
            if cat in text and cat not in q_lower:
                return False

        # Named entities hints
        for token in [
            "einstein",
            "cambridge",
            "sweden",
            "mit",
            "harvard",
            "paris",
            "stanford",
        ]:
            if token in text and token not in q_lower:
                return False

        # Relationship intent hints
        hints = {
            "affiliated": "affiliated_with",
            "born": "born_in",
            "located": "is_located_in",
        }
        for k, v in hints.items():
            if k in text and v not in q_lower:
                return False

        # If we reach here, execution succeeded and basic intent is reflected in the query
        return True

    def run_benchmark(self, config_name: str, **rag_kwargs) -> dict[str, Any]:
        """Run benchmark for a specific configuration using the proper GraphRAG interface."""
        print(f"Running {config_name} benchmark...")

        rag = GraphRAG(**rag_kwargs)
        results = []
        total_start = time.time()

        for question in self.test_questions:
            print(f"  Testing: {question[:60]}...")

            overall_start = time.time()

            try:
                # Use run_graph_rag which handles everything properly (like the main app)
                from graph_rag_lib import run_graph_rag

                result = run_graph_rag(
                    questions=[question], db_manager=self.db_manager, rag_instance=rag
                )[0]

                total_execution_time = time.time() - overall_start
                timing_info = result.get("timing_info", {})
                # Treat success as: query executed (result_count exists)
                success = "result_count" in timing_info
                timing_info = result.get("timing_info", {})

                # Compute query validity via EXPLAIN (independent of validation loop)
                query_valid = False
                try:
                    if success:
                        self.db_manager.conn.execute(f"EXPLAIN {result['query']}")
                        query_valid = True
                except Exception:
                    query_valid = False

                # Detailed result structure with timing breakdown
                bench_result = {
                    "question": question,
                    "total_time": total_execution_time,
                    "success": success,
                    "query": result["query"],
                    "timing_breakdown": timing_info,
                    "query_valid": query_valid,
                    "refine_iterations": timing_info.get("refine_iterations", 0),
                    "query_length": result.get(
                        "query_length", timing_info.get("query_length")
                    ),
                    "result_count": timing_info.get("result_count"),
                    "semantic_correct": self._assess_semantic(
                        question, result["query"], timing_info, result.get("context")
                    ),
                }

                # Print detailed breakdown
                print(
                    f"    Total: {total_execution_time:.3f}s ({'✓' if success else '✗'})"
                )
                if timing_info:
                    print(
                        f"    ├─ Relevance: {timing_info.get('relevance_check', 0):.3f}s"
                    )
                    if success:
                        cache_prune = (
                            " (cache)"
                            if timing_info.get("cache_hit_prune", False)
                            else ""
                        )
                        cache_query = (
                            " (cache)"
                            if timing_info.get("cache_hit_query", False)
                            else ""
                        )
                        print(
                            f"    ├─ Schema: {timing_info.get('schema_pruning', 0):.3f}s{cache_prune}"
                        )
                        print(
                            f"    ├─ Query: {timing_info.get('query_generation', 0):.3f}s{cache_query}"
                        )
                        if "post_processing" in timing_info:
                            print(
                                f"    ├─ Post-proc: {timing_info.get('post_processing', 0):.3f}s"
                            )
                        print(
                            f"    ├─ Execute: {timing_info.get('query_execution', 0):.3f}s"
                        )
                        print(
                            f"    └─ Answer: {timing_info.get('answer_generation', 0):.3f}s"
                        )
                    else:
                        print("    └─ Filtered out as irrelevant")
                else:
                    if not success:
                        print("    └─ Filtered out as irrelevant")

                results.append(bench_result)

            except Exception as e:
                total_execution_time = time.time() - overall_start
                bench_result = {
                    "question": question,
                    "total_time": total_execution_time,
                    "success": False,
                    "error": str(e),
                    "query": "",
                }
                results.append(bench_result)
                print(f"    ERROR: {str(e)[:80]}...")

        total_time = time.time() - total_start

        # Calculate aggregate statistics
        successful_results = [r for r in results if r["success"]]
        all_times = [r["total_time"] for r in results]
        valid_queries = [r for r in results if r.get("query_valid")]
        semantically_correct = [r for r in results if r.get("semantic_correct")]
        cache_prune_hits = sum(
            1
            for r in successful_results
            if r.get("timing_breakdown", {}).get("cache_hit_prune")
        )
        cache_query_hits = sum(
            1
            for r in successful_results
            if r.get("timing_breakdown", {}).get("cache_hit_query")
        )

        # Component timing averages (only for successful results)
        component_averages = {}
        if successful_results:
            for component in [
                "relevance_check",
                "schema_pruning",
                "query_generation",
                "query_execution",
                "answer_generation",
            ]:
                times = [
                    r["timing_breakdown"][component]
                    for r in successful_results
                    if "timing_breakdown" in r and component in r["timing_breakdown"]
                ]
                component_averages[component] = statistics.mean(times) if times else 0

        # Aggregate refine iteration stats and query length
        refine_iters = [r.get("refine_iterations", 0) for r in successful_results]
        query_lengths = [
            r.get("query_length")
            for r in successful_results
            if r.get("query_length") is not None
        ]

        summary = {
            "config": config_name,
            "total_time": total_time,
            "avg_time": statistics.mean(all_times),
            "median_time": statistics.median(all_times),
            "success_rate": len(successful_results) / len(results) if results else 0,
            "query_validity_rate": len(valid_queries) / len(results) if results else 0,
            "semantic_correct_rate": len(semantically_correct) / len(results)
            if results
            else 0,
            "questions_tested": len(results),
            "component_averages": component_averages,
            "avg_refine_iterations": statistics.mean(refine_iters)
            if refine_iters
            else 0,
            "median_refine_iterations": statistics.median(refine_iters)
            if refine_iters
            else 0,
            "avg_query_length": statistics.mean(query_lengths) if query_lengths else 0,
            "cache_hit_rate_prune": cache_prune_hits / len(successful_results)
            if successful_results
            else 0,
            "cache_hit_rate_query": cache_query_hits / len(successful_results)
            if successful_results
            else 0,
            "results": results,
        }

        # Print summary with component breakdown
        print(f"\n  === {config_name.upper()} SUMMARY ===")
        print(
            f"  Overall: {summary['avg_time']:.3f}s avg, {summary['success_rate'] * 100:.0f}% executed, {summary['query_validity_rate'] * 100:.0f}% syntax valid, {summary['semantic_correct_rate'] * 100:.0f}% semantic"
        )
        if component_averages:
            print("  Component breakdown (avg for successful queries):")
            print(
                f"    Relevance check: {component_averages.get('relevance_check', 0):.3f}s"
            )
            print(
                f"    Schema pruning:  {component_averages.get('schema_pruning', 0):.3f}s"
            )
            print(
                f"    Query generation:{component_averages.get('query_generation', 0):.3f}s"
            )
            if any(
                "post_processing" in r.get("timing_breakdown", {})
                for r in successful_results
            ):
                print(
                    f"    Post-processing:{statistics.mean([r['timing_breakdown'].get('post_processing', 0) for r in successful_results if 'timing_breakdown' in r]):.3f}s"
                )
            print(
                f"    Query execution: {component_averages.get('query_execution', 0):.3f}s"
            )
            print(
                f"    Answer generation:{component_averages.get('answer_generation', 0):.3f}s"
            )
            print("  Other metrics:")
            print(f"    Avg refine iterations: {summary['avg_refine_iterations']:.2f}")
            print(f"    Avg query length (tokens): {summary['avg_query_length']:.1f}")
            print(
                f"    Cache hit rate (prune/query): {summary['cache_hit_rate_prune'] * 100:.0f}% / {summary['cache_hit_rate_query'] * 100:.0f}%"
            )
        print()

        return summary


def main():
    import sys

    benchmark = GraphRAGBenchmark()

    if len(sys.argv) < 2:
        print("Usage: python benchmark.py <suite|baseline|validation|knn|compare>")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "baseline":
        result = benchmark.run_benchmark(
            "baseline", use_knn_fewshot=False, use_validation=False
        )

    elif mode == "validation":
        result = benchmark.run_benchmark(
            "validation", use_knn_fewshot=False, use_validation=True
        )

    elif mode == "knn":
        result = benchmark.run_benchmark(
            "knn", use_knn_fewshot=True, use_validation=True, k=3
        )

    elif mode == "compare":
        configs = [
            ("baseline", {"use_knn_fewshot": False, "use_validation": False}),
            ("validation", {"use_knn_fewshot": False, "use_validation": True}),
            ("knn", {"use_knn_fewshot": True, "use_validation": True, "k": 3}),
        ]

        results = {}
        for config_name, kwargs in configs:
            results[config_name] = benchmark.run_benchmark(config_name, **kwargs)

        # Print comparison
        print("=" * 80)
        print("DETAILED COMPARISON")
        print("=" * 80)

        # Detailed comparison table
        print(
            f"{'Config':<12} {'Total':<8} {'Success':<8} {'Valid%':<7} {'Relevance':<9} {'Schema':<8} {'Query':<8} {'Exec':<8} {'Answer':<8} {'Refine':<7} {'QLen':<6} {'CPrn':<6} {'CQry':<6}"
        )
        print("-" * 80)

        sorted_configs = sorted(results.items(), key=lambda x: x[1]["avg_time"])
        for config_name, data in sorted_configs:
            comp = data.get("component_averages", {})
            print(
                f"{config_name:<12} "
                f"{data['avg_time']:<8.3f} "
                f"{data['success_rate'] * 100:<8.0f}% "
                f"{data['query_validity_rate'] * 100:<7.0f}% "
                f"{comp.get('relevance_check', 0):<9.3f} "
                f"{comp.get('schema_pruning', 0):<8.3f} "
                f"{comp.get('query_generation', 0):<8.3f} "
                f"{comp.get('query_execution', 0):<8.3f} "
                f"{comp.get('answer_generation', 0):<8.3f} "
                f"{data.get('avg_refine_iterations', 0):<7.2f} "
                f"{data.get('avg_query_length', 0):<6.1f} "
                f"{data.get('cache_hit_rate_prune', 0) * 100:<6.0f}% "
                f"{data.get('cache_hit_rate_query', 0) * 100:<6.0f}%"
            )

        print("\nTimes are in seconds. Success is percentage of successful queries.")
        print(
            "Component breakdown shows average time per stage for successful queries only. Valid% is EXPLAIN success rate."
        )

        # Save results
        with open("benchmark_results.json", "w") as f:
            json.dump(results, f, indent=2)
        print("\nDetailed results saved to benchmark_results.json")
        return

    elif mode == "suite":
        # Full set of configurations requested
        suite_configs: list[tuple[str, dict[str, Any]]] = [
            (
                "Baseline (static few-shot)",
                {
                    "use_knn_fewshot": True,
                    "knn_static": True,
                    "use_validation": False,
                    "use_postprocessing": False,
                },
            ),
            (
                "Only Few-shot selection",
                {
                    "use_knn_fewshot": True,
                    "knn_static": False,
                    "use_validation": False,
                    "use_postprocessing": False,
                },
            ),
            (
                "Only Self-refinement",
                {
                    "use_knn_fewshot": False,
                    "use_validation": True,
                    "use_postprocessing": False,
                },
            ),
            (
                "Only Post-processing",
                {
                    "use_knn_fewshot": False,
                    "use_validation": False,
                    "use_postprocessing": True,
                },
            ),
            (
                "Few-shot + Post-processing",
                {
                    "use_knn_fewshot": True,
                    "knn_static": False,
                    "use_validation": False,
                    "use_postprocessing": True,
                },
            ),
            (
                "Few-shot + Refinement",
                {
                    "use_knn_fewshot": True,
                    "knn_static": False,
                    "use_validation": True,
                    "use_postprocessing": False,
                },
            ),
            (
                "Refinement + Post-processing",
                {
                    "use_knn_fewshot": False,
                    "use_validation": True,
                    "use_postprocessing": True,
                },
            ),
            (
                "All refinements",
                {
                    "use_knn_fewshot": True,
                    "knn_static": False,
                    "use_validation": True,
                    "use_postprocessing": True,
                },
            ),
        ]

        suite_results = {}
        print("\n=== RUNNING SUITE ===\n")
        for name, kwargs in suite_configs:
            suite_results[name] = benchmark.run_benchmark(name, **kwargs)

        # Print compact summary table with requested properties
        print("=" * 100)
        print("CONFIGURATION SUMMARY (requested metrics)")
        print("=" * 100)
        print(
            f"{'Config':<28} {'Syntax%':>8} {'Semantic%':>10} {'Avg Latency (ms)':>16} {'Avg Iters':>10}"
        )
        print("-" * 100)
        for name, data in suite_results.items():
            syntax_pct = data.get("query_validity_rate", 0) * 100
            semantic_pct = data.get("semantic_correct_rate", 0) * 100
            avg_latency_ms = data.get("avg_time", 0) * 1000
            avg_iters = data.get("avg_refine_iterations", 0)
            print(
                f"{name:<28} {syntax_pct:>8.0f}% {semantic_pct:>10.0f}% {avg_latency_ms:>16.0f} {avg_iters:>10.2f}"
            )

        # Save
        with open("benchmark_suite_results.json", "w") as f:
            json.dump(suite_results, f, indent=2)
        print("\nSuite results saved to benchmark_suite_results.json")
        return

    elif mode not in {"baseline", "validation", "knn"}:
        print(f"Unknown mode: {mode}")
        sys.exit(1)

    # Save single result
    filename = f"benchmark_{mode}.json"
    with open(filename, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Results saved to {filename}")


if __name__ == "__main__":
    main()
