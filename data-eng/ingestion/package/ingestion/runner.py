"""Orchestrate extract → transform → validate → load / dry-run."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ingestion.common.auth import AuthInfo, describe_auth, ensure_auth_safe, get_credentials
from ingestion.common.bq import get_client
from ingestion.common.config import Settings, get_settings
from ingestion.common.load import (
    add_metadata,
    load_dataframe,
    load_quarantine,
    new_run_id,
    write_run_log,
)
from ingestion.common.log import bind_run_id, get_logger
from ingestion.common.validate import validate_dataframe
from ingestion.registry import SourceSpec, get_source, load_source_modules

log = get_logger()


@dataclass
class RunContext:
    settings: Settings
    source: SourceSpec
    run_id: str
    since: str | None = None
    limit: int | None = None
    dry_run: bool = False
    client: Any | None = None


@dataclass
class RunResult:
    run_id: str
    source: str
    rows: int
    status: str
    out_path: Path | None = None
    errors: list[str] | None = None


def build_context(
    source_name: str,
    *,
    target: str | None = None,
    since: str | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    client: Any | None = None,
) -> RunContext:
    # Target guard runs inside get_settings — before any fetch/BQ.
    settings = get_settings(cli_target=target)
    ensure_auth_safe(settings)
    spec = get_source(source_name)
    run_id = new_run_id()
    bind_run_id(log, run_id)
    return RunContext(
        settings=settings,
        source=spec,
        run_id=run_id,
        since=since,
        limit=limit,
        dry_run=dry_run,
        client=client,
    )


def run_source(ctx: RunContext) -> RunResult:
    started = datetime.now(timezone.utc)
    extract_mod, transform_mod = load_source_modules(ctx.source)
    raw_chunks = list(extract_mod.extract(ctx))
    frames: list[pd.DataFrame] = []
    for raw in raw_chunks:
        part = transform_mod.transform(ctx, raw)
        if part is not None and not part.empty:
            frames.append(part)
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if ctx.since and ctx.source.since_column and ctx.source.since_column in df.columns:
        col = ctx.source.since_column
        df = df[pd.to_datetime(df[col], errors="coerce") >= pd.to_datetime(ctx.since)]

    if ctx.limit is not None and len(df) > ctx.limit:
        df = df.iloc[: ctx.limit].copy()

    expect = ctx.source.expect
    skip_min = bool(ctx.limit) or ctx.settings.offline_fixtures or ctx.dry_run
    result = validate_dataframe(
        df,
        required=list(expect.get("required") or []),
        min_rows=expect.get("min_rows"),
        skip_min_rows=skip_min,
    )

    source_url = ctx.source.url or ""
    good = add_metadata(
        result.good,
        source=ctx.source.name,
        run_id=ctx.run_id,
        source_url=source_url,
        key_cols=ctx.source.key_cols,
    )
    quarantine = result.quarantine
    if not quarantine.empty:
        quarantine = add_metadata(
            quarantine,
            source=ctx.source.name,
            run_id=ctx.run_id,
            source_url=source_url,
            key_cols=ctx.source.key_cols,
        )

    out_path: Path | None = None
    status = "ok"
    errors = list(result.errors)
    if not quarantine.empty:
        errors.append(f"{len(quarantine)} rows quarantined")

    if ctx.dry_run:
        ctx.settings.out_dir.mkdir(parents=True, exist_ok=True)
        out_path = ctx.settings.out_dir / f"{ctx.source.name}_{ctx.run_id}.parquet"
        good.to_parquet(out_path, index=False)
        log.info(
            "dry-run complete source=%s rows=%s schema=%s path=%s",
            ctx.source.name,
            len(good),
            list(good.columns),
            out_path,
        )
        if errors:
            status = "validation_failed"
            return RunResult(
                run_id=ctx.run_id,
                source=ctx.source.name,
                rows=len(good),
                status=status,
                out_path=out_path,
                errors=errors,
            )
        return RunResult(
            run_id=ctx.run_id,
            source=ctx.source.name,
            rows=len(good),
            status=status,
            out_path=out_path,
            errors=None,
        )

    # Live path — construct BQ client only here (never on dry-run).
    client = ctx.client
    if client is None:
        client = get_client(ctx.settings)

    auth: AuthInfo | None = None
    try:
        auth = describe_auth(ctx.settings)
    except Exception:
        pass
    principal = auth.principal if auth else "unknown"

    try:
        if not quarantine.empty:
            load_quarantine(client, ctx.settings, quarantine, table=ctx.source.table)
        if good.empty and errors:
            status = "validation_failed"
        else:
            load_dataframe(
                client,
                ctx.settings,
                good,
                table=ctx.source.table,
                mode=ctx.source.load_mode,
                run_id=ctx.run_id,
            )
            if errors:
                status = "validation_failed"
    except Exception as exc:
        status = "error"
        errors.append(str(exc))
        ended = datetime.now(timezone.utc)
        write_run_log(
            client,
            ctx.settings,
            run_id=ctx.run_id,
            source=ctx.source.name,
            rows=len(good),
            status=status,
            started=started,
            ended=ended,
            principal=principal,
            error="; ".join(errors),
        )
        raise

    ended = datetime.now(timezone.utc)
    write_run_log(
        client,
        ctx.settings,
        run_id=ctx.run_id,
        source=ctx.source.name,
        rows=len(good),
        status=status,
        started=started,
        ended=ended,
        principal=principal,
        error="; ".join(errors) if errors else None,
    )
    if status != "ok":
        return RunResult(
            run_id=ctx.run_id,
            source=ctx.source.name,
            rows=len(good),
            status=status,
            errors=errors,
        )
    return RunResult(
        run_id=ctx.run_id,
        source=ctx.source.name,
        rows=len(good),
        status=status,
    )


def check_auth(*, target: str | None = None, write_test: bool = False) -> AuthInfo:
    settings = get_settings(cli_target=target)
    ensure_auth_safe(settings)
    creds, _ = get_credentials(settings)
    info = describe_auth(settings, creds)
    if write_test:
        from ingestion.common.bq import write_test as bq_write_test

        client = get_client(settings)
        ref = bq_write_test(client, settings)
        log.info("write-test ok table=%s", ref)
    return info
