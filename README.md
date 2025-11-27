# CS-E4780-project

Course project for the Aalto University course CS-E4780 - Scalable Systems and Data Management D

## Project 1: Efficient Pattern Detection over Data Streams

This project implements a high-performance Complex Event Processing (CEP) system for detecting hot paths in bike-sharing data streams. Built on the OpenCEP framework, the system features pattern-aware load shedding that maintains high recall while meeting aggressive latency constraints. Key optimizations include reducing Kleene closure complexity from exponential to linear time and implementing a four-tier priority-based event filtering strategy.

The system processes CitiBike NYC trip data in real-time, detecting bike trip chains ending at high-demand stations. Performance evaluation shows 90.7% recall at 50% latency bound with throughput improvements from 148 to 325 events/sec under load shedding.

**[View Project 1 Details](project1/README.md)** | **[Full Report](project1/report/main.pdf)**

### Quick Start

```bash
cd project1/
make setup      # Install dependencies and prepare data
make run-demo   # Run demo with 1,000 events
make benchmark  # Generate benchmark results
```

See [project1/README.md](project1/README.md) for complete documentation, usage instructions, and development guidelines.

## Project 2: Enhancing LLM Inference with GraphRAG

This project implements a GraphRAG system that enhances LLM inference by integrating knowledge graph retrieval with natural language question answering over the Nobel Laureates dataset. Built on Kuzu graph database and DSPy framework, the system translates natural language questions into Cypher queries using three enhancement techniques: KNN-based dynamic few-shot exemplar selection, self-refinement with validation feedback, and rule-based post-processing.

The system achieves 100% syntax validity and 90% semantic correctness through an ablation study across eight enhancement configurations. LRU caching at schema pruning and query generation stages delivers a 270x latency improvement (1.77s → 6.5ms on cache hits), making interactive GraphRAG queries practical.

**[View Project 2 Details](project2/README.md)** | **[Full Report](project2/report/main.pdf)**

### Quick Start

```bash
cd project2/
uv sync                           # Install dependencies
uv run create_nobel_api_graph.py  # Create knowledge graph
uv run marimo run graph_rag.py    # Launch GraphRAG app
```

See [project2/README.md](project2/README.md) for complete documentation, benchmarking instructions, and evaluation details.
