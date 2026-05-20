"""
Thin CLI wrapper: use ``python -m ml.rag.run`` for the full RAG CLI (stderr SQL, retrieval summary, env via ``local_env``).

This module exists for backwards compatibility with ``python -m ml.rag.chatbot.run``.
"""
from __future__ import annotations

import sys
from pathlib import Path

from ml.rag.local_env import load_data_local_dotenv


def main() -> int:
    load_data_local_dotenv(Path(__file__).resolve().parents[2])
    from ml.rag.run import main as root_main

    return root_main()


if __name__ == "__main__":
    sys.exit(main())
