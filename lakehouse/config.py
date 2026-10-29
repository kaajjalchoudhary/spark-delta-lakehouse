"""Lake locations. Everything lives under one root so a run is easy to inspect or delete."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Lake:
    root: Path

    @classmethod
    def from_env(cls) -> Lake:
        return cls(Path(os.getenv("LAKE_ROOT", "lake")).resolve())

    @property
    def landing(self) -> Path:
        return self.root / "landing"

    def table(self, layer: str, name: str) -> str:
        """Path of a Delta table, e.g. lake.table("silver", "trips")."""
        return str(self.root / layer / name)

    @property
    def runs(self) -> Path:
        return self.root / "_runs"
