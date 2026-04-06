"""
Entrypoint to run the RAG pipeline. Usage:
  PYTHONPATH=. python -m ml.rag.run "your question here"
  PYTHONPATH=. python -m ml.rag.run
"""
from __future__ import annotations

import sys
from pathlib import Path

# Allow loading .env from data/local when running from repo root
_repo_root = Path(__file__).resolve().parents[2]
_env = _repo_root / "data" / "local" / ".env"
if _env.exists():
    with open(_env) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and k not in __import__("os").environ:
                    __import__("os").environ[k] = v


def main() -> int:
    from ml.rag.graph import run_rag

    query = " ".join(sys.argv[1:]).strip() if len(sys.argv) > 1 else "What bronze tables can we query for yields and food security?"
    try:
        result = run_rag(query)
        answer = result.get("answer", "")
        print(answer)
        if result.get("error"):
            print("Error:", result["error"], file=sys.stderr)
        return 0 if not result.get("error") else 1
    except Exception as e:
        print(f"RAG failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
