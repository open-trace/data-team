"""Keep the RAG test suite hermetic: no local .env files and no live service credentials.

Importing ``app.api`` or ``streamlit_app`` loads ``config/.env`` and ``data/local/.env``,
which would point tests at live LLM, BigQuery and Qdrant services. CI has none of these,
so local runs must match it.
"""

from __future__ import annotations

import os

import ml.rag.local_env as local_env

_SERVICE_ENV_PREFIXES = ("RAG_", "QDRANT_", "OPENROUTER_", "BQ_", "GOOGLE_", "HF_", "LANGFUSE_", "ADZA_")

for _key in list(os.environ):
    if _key.startswith(_SERVICE_ENV_PREFIXES):
        del os.environ[_key]

# Skip only the file reads; the rest of load_rag_dotenv (credential bootstrap) stays testable.
local_env._merge_dotenv_file = lambda *args, **kwargs: None
