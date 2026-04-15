# Shared Python libraries

Put reusable pipeline code here (BigQuery clients, Airbyte API wrappers, structured logging, data-quality helpers) and install via editable package or `PYTHONPATH` in Composer/Docker images.

Avoid importing from **`ml/`** here — keep ML dependencies out of the data-eng runtime where possible.
