#!/usr/bin/env python3
"""Discover current BigQuery tables/views for local PostgreSQL population.

Every layer uses live dataset metadata. An explicit BQ_{LAYER}_TABLES value
restricts a layer for a targeted run. Static dbt/RAG catalogs are not consulted.
"""
from __future__ import annotations

import os

LAYERS = ('bronze', 'silver', 'gold')
DATASET_ENV = {
    'bronze': 'BQ_DATASET_BRONZE',
    'silver': 'BQ_DATASET_SILVER',
    'gold': 'BQ_DATASET_GOLD',
}


def dataset_id_for_layer(layer: str) -> str:
    if layer not in LAYERS:
        raise ValueError(f'Unknown layer: {layer}')
    defaults = {'bronze': 'raw_dev', 'silver': 'staging_dev', 'gold': 'mart_dev'}
    return os.environ.get(DATASET_ENV[layer], '').strip() or defaults[layer]


def local_schema_for_layer(layer: str) -> str:
    return dataset_id_for_layer(layer)


def load_layer_tables(layer: str, *, client=None) -> list[str]:
    dataset_id = dataset_id_for_layer(layer)
    override = os.environ.get(f'BQ_{layer.upper()}_TABLES', '').strip()
    if override:
        return sorted({name.strip() for name in override.split(',') if name.strip()})
    project = os.environ.get('BQ_PROJECT', '').strip()
    if not project:
        raise ValueError('Set BQ_PROJECT before discovering warehouse tables')
    if client is None:
        from google.cloud import bigquery
        client = bigquery.Client(project=project)
    timeout = int(os.environ.get('BQ_METADATA_TIMEOUT_SECONDS', '15'))
    return sorted(table.table_id for table in client.list_tables(
        f'{project}.{dataset_id}', timeout=timeout))
