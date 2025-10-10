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
