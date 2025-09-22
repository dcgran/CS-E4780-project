"""Generate synthetic CSV-like event streams for experimentation.

Each event line format (simple):
Type,Peak Price,Opening Price,Other

Usage programmatically: call generate(...)
CLI (future TODO) not yet implemented.
"""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import random
from typing import Sequence


def generate(path: str | Path, n: int, event_types: Sequence[str], seed: int | None = None):
    rng = random.Random(seed)
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open('w') as f:
        for _ in range(n):
            et = rng.choice(event_types)
            peak = round(rng.uniform(10, 1000), 2)
            opening = round(peak - rng.uniform(0, 5), 2)
            other = rng.randint(0, 100)
            f.write(f"{et},{peak},{opening},{other}\n")

__all__ = ["generate"]
