"""Реестр источников данных: host/port/database/schema из config/sources.yaml."""
from __future__ import annotations

import os
from pathlib import Path

import yaml

_SOURCES_CONFIG = os.environ.get("SOURCES_CONFIG", "/app/config/sources.yaml")

_sources: dict[str, dict] = {}


def load_sources(path: str | None = None) -> dict[str, dict]:
    global _sources
    p = Path(path or _SOURCES_CONFIG)
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    _sources = {name: cfg for name, cfg in raw["sources"].items()}
    return _sources


def get_source(name: str) -> dict:
    if name not in _sources:
        raise KeyError(name)
    return _sources[name]


def source_names() -> list[str]:
    return list(_sources.keys())