"""Preserve BigQuery types when creating and loading local PostgreSQL tables."""
from __future__ import annotations

from collections.abc import Mapping
import json
import hashlib

from sqlalchemy import types
from sqlalchemy.dialects.postgresql import JSONB


def local_table_name(name: str) -> str:
    """Fit PostgreSQL's 63-byte limit without merging distinct source tables."""
    encoded = name.encode('utf-8')
    if len(encoded) <= 63:
        return name
    prefix = encoded[:54].decode('utf-8', errors='ignore')
    return prefix + '_' + hashlib.sha256(encoded).hexdigest()[:8]


def postgres_type(field_type: str, mode: str | None = None):
    kind = (field_type or 'STRING').upper()
    if kind in {'JSON', 'RECORD', 'STRUCT', 'ARRAY'} or (mode or '').upper() == 'REPEATED':
        return JSONB()
    mapping = {
        'STRING': types.Text, 'INTEGER': types.BigInteger, 'INT64': types.BigInteger,
        'FLOAT': types.Double, 'FLOAT64': types.Double,
        'BOOL': types.Boolean, 'BOOLEAN': types.Boolean,
        'DATE': types.Date, 'TIME': types.Time, 'DATETIME': types.DateTime,
        'NUMERIC': types.Numeric, 'BIGNUMERIC': types.Numeric,
        'BYTES': types.LargeBinary, 'GEOGRAPHY': types.Text,
    }
    if kind == 'TIMESTAMP':
        return types.DateTime(timezone=True)
    return mapping.get(kind, types.Text)()


def _json_compatible(value):
    if isinstance(value, Mapping) or hasattr(value, 'items'):
        return {str(k): _json_compatible(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_compatible(v) for v in value]
    return value


def value_for_field(value, field):
    if value is None:
        return None
    if isinstance(postgres_type(field.field_type, field.mode), JSONB):
        if field.field_type.upper() == 'JSON' and isinstance(value, str):
            return json.loads(value)
        # Nested Decimal/date values retain their string representations.
        # Return native JSON, not a JSON string that the driver encodes again.
        return json.loads(json.dumps(_json_compatible(value), default=str))
    return value
