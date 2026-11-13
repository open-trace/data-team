"""CLI: list | check-auth | run."""

from __future__ import annotations

import argparse
import json
import sys

from ingestion.common.auth import AuthError, auth_info_as_dict
from ingestion.common.config import TargetGuardError
from ingestion.registry import list_sources
from ingestion.runner import build_context, check_auth, run_source


def _cmd_list(_: argparse.Namespace) -> int:
    specs = list_sources()
    if not specs:
        print("No sources found.")
        return 0
    for s in specs:
        print(f"{s.name}\t{s.table}\t{s.load_mode}")
    return 0


def _cmd_check_auth(args: argparse.Namespace) -> int:
    try:
        info = check_auth(target=args.target, write_test=args.write_test)
    except (TargetGuardError, AuthError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(auth_info_as_dict(info), indent=2))
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    try:
        ctx = build_context(
            args.source,
            target=args.target,
            since=args.since,
            limit=args.limit,
            dry_run=args.dry_run,
        )
        result = run_source(ctx)
    except TargetGuardError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except AuthError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except KeyError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "source": result.source,
                "rows": result.rows,
                "status": result.status,
                "out_path": str(result.out_path) if result.out_path else None,
                "errors": result.errors,
            },
            indent=2,
        )
    )
    if result.status != "ok":
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ingestion", description="OpenTrace BigQuery ingestion")
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list", help="List registered sources")
    p_list.set_defaults(func=_cmd_list)

    p_auth = sub.add_parser("check-auth", help="Print active principal and target")
    p_auth.add_argument("--target", default=None, help="Override target dataset")
    p_auth.add_argument(
        "--write-test",
        action="store_true",
        help="Create and drop a temp table in the target dataset",
    )
    p_auth.set_defaults(func=_cmd_check_auth)

    p_run = sub.add_parser("run", help="Run a source")
    p_run.add_argument("source", help="Source name (see `list`)")
    p_run.add_argument("--since", default=None, help="Filter since YYYY-MM-DD")
    p_run.add_argument("--limit", type=int, default=None, help="Cap rows")
    p_run.add_argument("--dry-run", action="store_true", help="No BigQuery writes")
    p_run.add_argument("--target", default=None, help="Target dataset")
    p_run.set_defaults(func=_cmd_run)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
