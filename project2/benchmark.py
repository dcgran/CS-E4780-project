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
                    questions=[question],
                    db_manager=self.db_manager,
                    rag_instance=rag
                )[0]
                
                total_execution_time = time.time() - overall_start
                success = result["query"] != "N/A - Question not relevant to domain"
                timing_info = result.get("timing_info", {})
                
                # Detailed result structure with timing breakdown
                bench_result = {
                    "question": question,
                    "total_time": total_execution_time,
                    "success": success,
                    "query": result["query"],
                    "timing_breakdown": timing_info
                }
                
                # Print detailed breakdown
                print(f"    Total: {total_execution_time:.3f}s ({'✓' if success else '✗'})")
                if timing_info:
                    print(f"    ├─ Relevance: {timing_info.get('relevance_check', 0):.3f}s")
                    if success:
                        cache_prune = " (cache)" if timing_info.get('cache_hit_prune', False) else ""
                        cache_query = " (cache)" if timing_info.get('cache_hit_query', False) else ""
                        print(f"    ├─ Schema: {timing_info.get('schema_pruning', 0):.3f}s{cache_prune}")
                        print(f"    ├─ Query: {timing_info.get('query_generation', 0):.3f}s{cache_query}")
                        print(f"    ├─ Execute: {timing_info.get('query_execution', 0):.3f}s")
                        print(f"    └─ Answer: {timing_info.get('answer_generation', 0):.3f}s")
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
                    "query": ""
                }
                results.append(bench_result)
                print(f"    ERROR: {str(e)[:80]}...")
        
        total_time = time.time() - total_start
        
        # Calculate aggregate statistics
        successful_results = [r for r in results if r["success"]]
        all_times = [r["total_time"] for r in results]
        
        # Component timing averages (only for successful results)
        component_averages = {}
        if successful_results:
            for component in ["relevance_check", "schema_pruning", "query_generation", "query_execution", "answer_generation"]:
                times = [r["timing_breakdown"][component] for r in successful_results if "timing_breakdown" in r and component in r["timing_breakdown"]]
                component_averages[component] = statistics.mean(times) if times else 0
        
        summary = {
            "config": config_name,
            "total_time": total_time,
            "avg_time": statistics.mean(all_times),
            "median_time": statistics.median(all_times),
            "success_rate": len(successful_results) / len(results),
            "questions_tested": len(results),
            "component_averages": component_averages,
            "results": results
        }
        
        # Print summary with component breakdown
        print(f"\n  === {config_name.upper()} SUMMARY ===")
        print(f"  Overall: {summary['avg_time']:.3f}s avg, {summary['success_rate']*100:.0f}% success")
        if component_averages:
            print("  Component breakdown (avg for successful queries):")
            print(f"    Relevance check: {component_averages.get('relevance_check', 0):.3f}s")
            print(f"    Schema pruning:  {component_averages.get('schema_pruning', 0):.3f}s") 
            print(f"    Query generation:{component_averages.get('query_generation', 0):.3f}s")
            print(f"    Query execution: {component_averages.get('query_execution', 0):.3f}s")
            print(f"    Answer generation:{component_averages.get('answer_generation', 0):.3f}s")
        print()
        
        return summary


def main():
    import sys
    
    benchmark = GraphRAGBenchmark()
    
    if len(sys.argv) < 2:
        print("Usage: python benchmark.py <baseline|validation|knn|compare>")
        sys.exit(1)
    
    mode = sys.argv[1]
    
    if mode == "baseline":
        result = benchmark.run_benchmark("baseline", use_knn_fewshot=False, use_validation=False)
        
    elif mode == "validation":
        result = benchmark.run_benchmark("validation", use_knn_fewshot=False, use_validation=True)
        
    elif mode == "knn":
        result = benchmark.run_benchmark("knn", use_knn_fewshot=True, use_validation=True, k=3)
        
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
        print(f"{'Config':<12} {'Total':<8} {'Success':<8} {'Relevance':<9} {'Schema':<8} {'Query':<8} {'Execute':<8} {'Answer':<8}")
        print("-" * 80)
        
        sorted_configs = sorted(results.items(), key=lambda x: x[1]["avg_time"])
        for config_name, data in sorted_configs:
            comp = data.get("component_averages", {})
            print(f"{config_name:<12} "
                  f"{data['avg_time']:<8.3f} "
                  f"{data['success_rate']*100:<8.0f}% "
                  f"{comp.get('relevance_check', 0):<9.3f} "
                  f"{comp.get('schema_pruning', 0):<8.3f} "
                  f"{comp.get('query_generation', 0):<8.3f} "
                  f"{comp.get('query_execution', 0):<8.3f} "
                  f"{comp.get('answer_generation', 0):<8.3f}")
        
        print("\nTimes are in seconds. Success is percentage of successful queries.")
        print("Component breakdown shows average time per stage for successful queries only.")
        
        # Save results
        with open("benchmark_results.json", "w") as f:
            json.dump(results, f, indent=2)
        print("\nDetailed results saved to benchmark_results.json")
        return
    
    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)
    
    # Save single result
    filename = f"benchmark_{mode}.json"
    with open(filename, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Results saved to {filename}")


if __name__ == "__main__":
    main()
