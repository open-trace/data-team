# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Load secrets at request time; never include them in source profiles or logs."""

import json
import os
from pathlib import Path
from urllib.parse import urlsplit

CREDENTIALS_FILE = Path(__file__).resolve().parents[2] / "config/tls/api_key.json"


def api_key(api_name, environment_variable):
    """Read one named credential; deployment environment values take precedence."""
    key = os.environ.get(environment_variable, "").strip()
    if not key and CREDENTIALS_FILE.exists():
        try:
            credentials = json.loads(CREDENTIALS_FILE.read_text(encoding="utf-8"))
        except (ValueError, UnicodeError) as error:
            raise ValueError("API credential file must contain valid JSON.") from None
        if not isinstance(credentials, dict):
            raise ValueError("API credential file must map API names to credential objects.")
        settings = credentials.get(api_name, {})
        if not isinstance(settings, dict):
            raise ValueError(f"Credentials for {api_name} must be an object.")
        key = settings.get("api_key", "")
    if not isinstance(key, str):
        raise ValueError(f"The {api_name} api_key must be a string.")
    if any(char in key for char in "\r\n"):
        raise ValueError(f"The {api_name} key must be a single line.")
    return key.strip()


def request_headers(url):
    """Send the API-Ninjas key only to its exact HTTPS API host."""
    parts = urlsplit(url)
    if parts.scheme != "https" or parts.netloc != "api.api-ninjas.com":
        return {}
    key = api_key("API_NINJAS", "API_NINJAS_KEY")
    return {"X-Api-Key": key} if key else {}
