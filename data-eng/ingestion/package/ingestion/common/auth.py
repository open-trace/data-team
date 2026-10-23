"""Describe and obtain Google credentials (shared SA key primary)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import google.auth
from google.auth import impersonated_credentials
from google.auth.credentials import Credentials

from ingestion.common.config import REPO_ROOT, Settings, credentials_path_inside_repo


class AuthError(RuntimeError):
    """Raised when credentials are misconfigured."""


@dataclass(frozen=True)
class AuthInfo:
    principal: str
    kind: str  # key_file | impersonated_sa | service_account | user | unknown
    project: str
    location: str
    target: str


def _refuse_in_repo_key(repo_root: Path | None = None) -> None:
    cred = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_path_inside_repo(cred, repo_root):
        raise AuthError(
            f"GOOGLE_APPLICATION_CREDENTIALS points inside the repo ({cred}). "
            "Move the key outside the repository (e.g. ~/.config/opentrace/ingest-dev.json)."
        )


def _already_impersonating(creds: Credentials, target_sa: str) -> bool:
    if isinstance(creds, impersonated_credentials.Credentials):
        email = getattr(creds, "service_account_email", None) or getattr(
            creds, "_service_account_email", None
        )
        return email == target_sa
    email = getattr(creds, "service_account_email", None)
    return email == target_sa


def get_credentials(
    settings: Settings,
    *,
    scopes: list[str] | None = None,
) -> tuple[Credentials, str | None]:
    """Return credentials. Prefer GOOGLE_APPLICATION_CREDENTIALS (shared SA key)."""
    _refuse_in_repo_key(settings.repo_root)
    scope_list = scopes or ["https://www.googleapis.com/auth/cloud-platform"]
    source_creds, project_id = google.auth.default(scopes=scope_list)

    # Optional legacy path only if explicitly set — not used for team onboarding.
    if settings.impersonate_sa:
        if _already_impersonating(source_creds, settings.impersonate_sa):
            return source_creds, project_id or settings.project
        target = impersonated_credentials.Credentials(
            source_credentials=source_creds,
            target_principal=settings.impersonate_sa,
            target_scopes=scope_list,
            lifetime=3600,
        )
        return target, project_id or settings.project

    return source_creds, project_id or settings.project


def describe_auth(settings: Settings, credentials: Credentials | None = None) -> AuthInfo:
    _refuse_in_repo_key(settings.repo_root)
    creds = credentials
    if creds is None:
        creds, _ = get_credentials(settings)

    # Key file is the team-standard path — report it first when set.
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") and not settings.impersonate_sa:
        return AuthInfo(
            principal=Path(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]).name,
            kind="key_file",
            project=settings.project,
            location=settings.location,
            target=settings.target_dataset,
        )

    kind = "unknown"
    principal = "unknown"

    if isinstance(creds, impersonated_credentials.Credentials):
        kind = "impersonated_sa"
        principal = getattr(creds, "service_account_email", None) or settings.impersonate_sa or "?"
    elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        kind = "key_file"
        principal = Path(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]).name
    else:
        email = getattr(creds, "service_account_email", None)
        if email:
            kind = "service_account"
            principal = email
        else:
            kind = "user"
            principal = getattr(creds, "signer_email", None) or os.environ.get(
                "CLOUDSDK_CORE_ACCOUNT", "adc-user"
            )

    if settings.impersonate_sa and kind != "impersonated_sa":
        principal = settings.impersonate_sa
        kind = "impersonated_sa"

    return AuthInfo(
        principal=str(principal),
        kind=kind,
        project=settings.project,
        location=settings.location,
        target=settings.target_dataset,
    )


def auth_info_as_dict(info: AuthInfo) -> dict[str, Any]:
    return {
        "principal": info.principal,
        "kind": info.kind,
        "project": info.project,
        "location": info.location,
        "target": info.target,
    }


def ensure_auth_safe(settings: Settings | None = None) -> None:
    """Call at process start to refuse in-repo key files."""
    root = settings.repo_root if settings else REPO_ROOT
    _refuse_in_repo_key(root)
