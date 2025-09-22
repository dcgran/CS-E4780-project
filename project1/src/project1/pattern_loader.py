"""Utility to load pattern definitions from a simple YAML/JSON spec into OpenCEP Pattern objects.

Schema (YAML or JSON):

patterns:
  - name: google_ascent
    structure: SEQ
    events:
      - { type: GOOG, alias: a }
      - { type: GOOG, alias: b }
      - { type: GOOG, alias: c }
    condition:
      all: # list of atomic binary comparisons executed left->right, variable names must exist
        - { op: <, left: a.Peak Price, right: b.Peak Price }
        - { op: <, left: b.Peak Price, right: c.Peak Price }
    within: 180s  # or '3m', '300ms', '2h'
    confidence: 0.0  # optional

Supported condition kinds:
  all: list of binary predicates combined with AndCondition
  any: list combined with OrCondition (TODO: not yet implemented)

Supported operators: <, <=, >, >=, ==, !=

Time format parsing supports suffix: s, ms, m, h.

Limitations / TODO:
 - OrCondition support
 - Kleene closure, negation, and, etc. advanced constructs via structure spec
 - Selection strategies & consumption policies from config

"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Sequence, Callable
import json
import re
from pathlib import Path

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover - yaml is optional
    yaml = None  # type: ignore

from opencep.base.Pattern import Pattern
from opencep.base.PatternStructure import PrimitiveEventStructure, SeqOperator
from opencep.condition.Condition import Variable
from opencep.condition.CompositeCondition import AndCondition
from opencep.condition.BaseRelationCondition import (
    SmallerThanCondition, SmallerThanEqCondition, GreaterThanCondition, GreaterThanEqCondition, EqCondition, NotEqCondition
)

_OPERATOR_MAP: Dict[str, Callable[[Variable, Variable | Any], Any]] = {
    '<': SmallerThanCondition,
    '<=': SmallerThanEqCondition,
    '>': GreaterThanCondition,
    '>=': GreaterThanEqCondition,
    '==': EqCondition,
    '!=': NotEqCondition,
}


def _parse_time(s: str) -> timedelta:
    m = re.fullmatch(r"(\d+)(ms|s|m|h)", s.strip())
    if not m:
        raise ValueError(f"Invalid time format '{s}'")
    value = int(m.group(1))
    unit = m.group(2)
    if unit == 'ms':
        return timedelta(milliseconds=value)
    if unit == 's':
        return timedelta(seconds=value)
    if unit == 'm':
        return timedelta(minutes=value)
    if unit == 'h':
        return timedelta(hours=value)
    raise ValueError(unit)


def _parse_attr_ref(ref: str):
    # form alias.Field Name (field may contain spaces): split only first dot
    if '.' not in ref:
        raise ValueError(f"Attribute reference must be alias.field got '{ref}'")
    alias, field = ref.split('.', 1)
    field = field.strip()
    return alias.strip(), field


@dataclass
class LoadedPattern:
    name: str
    pattern: Pattern


def load_patterns(path: str | Path) -> List[LoadedPattern]:
    p = Path(path)
    raw: Dict[str, Any]
    if p.suffix.lower() in {'.yaml', '.yml'}:
        if yaml is None:
            raise RuntimeError("pyyaml not installed")
        raw = yaml.safe_load(p.read_text())
    else:
        raw = json.loads(p.read_text())
    patterns_spec = raw.get('patterns') or []
    results: List[LoadedPattern] = []
    for spec in patterns_spec:
        name = spec.get('name') or 'pattern'
        structure = spec.get('structure', 'SEQ').upper()
        events = spec.get('events', [])
        within_spec = spec.get('within', '60s')
        confidence = spec.get('confidence')

        structures_nodes = []
        for ev in events:
            structures_nodes.append(PrimitiveEventStructure(ev['type'], ev['alias']))
        if structure == 'SEQ':
            structure_node = SeqOperator(*structures_nodes)
        else:
            raise NotImplementedError(f"Structure {structure} not implemented yet")

        # Build conditions
        cond_spec = spec.get('condition', {})
        all_conds = cond_spec.get('all', [])
        cond_objects = []
        for c in all_conds:
            op = c['op']
            left_alias, left_field = _parse_attr_ref(c['left'])
            right = c['right']
            # Determine right operand
            if isinstance(right, str):
                if right.startswith('CONST:'):
                    right_var: Variable | Any = right.split(':', 1)[1]
                elif '.' in right and right[0].isalnum():
                    # attempt field ref
                    try:
                        r_alias, r_field = _parse_attr_ref(right)
                        right_var = Variable(r_alias, lambda x, f=r_field: x[f])
                    except ValueError:
                        right_var = right
                else:
                    # plain string literal
                    right_var = right
            else:
                right_var = right
            left_var = Variable(left_alias, lambda x, f=left_field: x[f])
            relation_cls = _OPERATOR_MAP[op]
            cond_objects.append(relation_cls(left_var, right_var))
        if cond_objects:
            # fold into AndCondition
            from functools import reduce
            from opencep.condition.CompositeCondition import AndCondition as AC
            condition = reduce(lambda a, b: AC(a, b), cond_objects)
        else:
            from opencep.condition.CompositeCondition import DummyCondition  # type: ignore
            condition = DummyCondition()

        td = _parse_time(within_spec)
        pattern_obj = Pattern(structure_node, condition, td, confidence=confidence)
        results.append(LoadedPattern(name=name, pattern=pattern_obj))
    return results

__all__ = ["load_patterns", "LoadedPattern"]
