"""FQN-to-target table name resolution for raw-layer Iceberg tables.

Every dot in an entity FQN becomes the "__" delimiter: the structural
schema.table separator and dots embedded inside quoted fragments alike.
"""

import re
from collections.abc import Iterable

CATALOG = "raw"
DELIMITER = "__"

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


def _split_fragments(fqn: str) -> list[str]:
    """Split an FQN into dot-separated fragments, honoring double-quoted parts."""
    fragments: list[str] = []
    current: list[str] = []
    in_quotes = False
    for char in fqn:
        if char == '"':
            in_quotes = not in_quotes
        elif char == "." and not in_quotes:
            fragments.append("".join(current))
            current = []
        else:
            current.append(char)
    if in_quotes:
        raise ValueError(f"unbalanced quotes in FQN: {fqn!r}")
    fragments.append("".join(current))
    if any(fragment == "" for fragment in fragments):
        raise ValueError(f"empty fragment in FQN: {fqn!r}")
    return fragments


def _validate_identifier(value: str, kind: str, source: str) -> str:
    """Validate a lowercase identifier and raise ValueError with full context."""
    if not _IDENTIFIER.match(value):
        raise ValueError(f"{kind} {value!r} resolved from {source!r} is not a valid identifier")
    return value


def resolve_target_table_name(fqn: str) -> str:
    """Resolve a two-fragment RDBMS FQN (<schema>.<table>) to a raw-layer table name."""
    fragments = _split_fragments(fqn)
    if len(fragments) != 2:
        raise ValueError(
            f"FQN must contain exactly 2 fragments (schema.table), got {len(fragments)}: {fqn!r}"
        )
    encoded = [fragment.lower().replace(".", DELIMITER) for fragment in fragments]
    target = DELIMITER.join(encoded)
    return _validate_identifier(target, "target table name", fqn)


def resolve_namespace(source_id: str) -> str:
    """Resolve a source id to its raw-layer namespace (schema inside the raw catalog)."""
    return _validate_identifier(source_id.lower(), "namespace", source_id)


def resolve_fully_qualified(source_id: str, fqn: str) -> str:
    """Resolve a source id and entity FQN to the full three-part table identifier."""
    return f"{CATALOG}.{resolve_namespace(source_id)}.{resolve_target_table_name(fqn)}"


def assert_unique_targets(fqns: Iterable[str]) -> None:
    """Assert that the given FQNs resolve to pairwise-unique target table names."""
    seen: dict[str, str] = {}
    for fqn in fqns:
        target = resolve_target_table_name(fqn)
        if target in seen:
            raise ValueError(f"FQNs {seen[target]!r} and {fqn!r} both resolve to target {target!r}")
        seen[target] = fqn
