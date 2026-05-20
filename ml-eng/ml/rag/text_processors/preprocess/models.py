from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ChunkOutput(BaseModel):
    """Unified JSONL row for all corpora."""

    id: str
    text: str
    metadata: dict[str, Any] = Field(default_factory=dict)

    def to_jsonl_dict(self) -> dict[str, Any]:
        return {"id": self.id, "text": self.text, "metadata": self.metadata}
