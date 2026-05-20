from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DRIVE_READONLY_SCOPES = ("https://www.googleapis.com/auth/drive.readonly",)


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


@dataclass(frozen=True)
class DriveAuthConfig:
    client_secret_json_path: Path
    token_path: Path
    scopes: tuple[str, ...] = DRIVE_READONLY_SCOPES


def _ml_eng_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _resolve_ml_eng_path(raw: str) -> Path:
    p = Path(raw).expanduser()
    if p.is_absolute():
        return p.resolve()
    return (_ml_eng_root() / p).resolve()


def load_auth_config_from_env() -> DriveAuthConfig:
    """
    Env:
      - GDRIVE_OAUTH_CLIENT_SECRET_JSON: path to OAuth client secret json downloaded from GCP console
      - GDRIVE_TOKEN_PATH: path to persist the user token (default: ml-eng/data/local/gdrive_token.json)
    """
    raw_secret = _env("GDRIVE_OAUTH_CLIENT_SECRET_JSON")
    if not raw_secret:
        raise RuntimeError("Set GDRIVE_OAUTH_CLIENT_SECRET_JSON to the OAuth client secret JSON path.")

    token_path = _env("GDRIVE_TOKEN_PATH")
    if token_path:
        tp = _resolve_ml_eng_path(token_path)
    else:
        tp = (_ml_eng_root() / "data" / "local" / "gdrive_token.json").resolve()

    return DriveAuthConfig(
        client_secret_json_path=_resolve_ml_eng_path(raw_secret),
        token_path=tp,
        scopes=DRIVE_READONLY_SCOPES,
    )


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def build_drive_service(config: DriveAuthConfig):
    """
    Build an authenticated Google Drive API service.

    Uses OAuth user auth (InstalledAppFlow) with a persisted token cache.
    """
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
    except ImportError as e:
        raise ImportError(
            "Missing Google Drive deps. Install: google-api-python-client google-auth-oauthlib"
        ) from e

    creds = None
    if config.token_path.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(config.token_path), scopes=list(config.scopes))
        except Exception:
            creds = None

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    elif not creds or not creds.valid:
        if not config.client_secret_json_path.exists():
            raise RuntimeError(f"OAuth client secret json not found: {config.client_secret_json_path}")
        # Use the standard "installed app" flow; opens a browser locally.
        flow = InstalledAppFlow.from_client_secrets_file(
            str(config.client_secret_json_path),
            scopes=list(config.scopes),
        )
        creds = flow.run_local_server(port=0)

    config.token_path.parent.mkdir(parents=True, exist_ok=True)
    config.token_path.write_text(creds.to_json(), encoding="utf-8")

    return build("drive", "v3", credentials=creds, cache_discovery=False)

