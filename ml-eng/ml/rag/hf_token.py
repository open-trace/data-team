"""Resolve Hugging Face API tokens from common environment variable names."""
from __future__ import annotations

import os

# Order: project-specific first, then Hugging Face Hub / CLI defaults.
_HF_TOKEN_KEYS = ("HF_API_TOKEN", "HUGGINGFACE_HUB_TOKEN", "HF_TOKEN")


def get_hf_api_token() -> str:
    """First non-empty token from HF_API_TOKEN, HUGGINGFACE_HUB_TOKEN, or HF_TOKEN."""
    for key in _HF_TOKEN_KEYS:
        v = os.environ.get(key, "").strip()
        if v:
            return v
    return ""
