"""
BigQuery retriever: natural-language questions → BigQuery SQL over the bronze dataset only.
Uses Llama 3.1 via Hugging Face for NL-to-SQL; validates and runs only SELECTs.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import requests

from ml.rag.retrievers.base import BaseRetriever

# Load .env when used from repo root
_REPO_ROOT = Path(__file__).resolve().parents[3]
_ENV_FILE = _REPO_ROOT / "data" / "local" / ".env"


def _load_dotenv() -> None:
    if not _ENV_FILE.exists():
        return
    for line in _ENV_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip().replace("export ", "", 1).strip()
        value = value.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = value


def _get_datasets_config() -> dict[str, str]:
    """Bronze dataset IDs from env (for NL-to-SQL and validation)."""
    return {
        "bronze": os.environ.get("BQ_DATASET_BRONZE", "bronze").strip(),
    }


def _call_llama_for_sql(prompt: str) -> str:
    """Call LLM via Hugging Face router (chat completions); return raw text (expected to be SQL)."""
    api_token = os.environ.get("HF_API_TOKEN")
    model_id = os.environ.get("RAG_LLM_MODEL_ID", "meta-llama/Llama-3.1-8B-Instruct")
    if not api_token:
        return ""
    url = "https://router.huggingface.co/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_token}", "Accept": "application/json", "Content-Type": "application/json"}
    payload = {
        "model": model_id,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 512,
        "temperature": 0.0,
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return str(data["choices"][0]["message"]["content"]).strip()
    except Exception:
        return ""


# Filter-column mapping aligned with bronze dbt sources. Use exact names from the Schema section.
_SCHEMA_FILTER_GUIDE = """
Filter columns by question intent (use exact column names from the schema below):
- Country/region: country, country_code, country_name, area (FAO tables), adm0_name, reporting_country, Reference area, geographic_unit_name, fewsnet_region, admin_1, admin_2, admin_0
- Season / time: season_name, planting_year, harvest_year, year, TIME_PERIOD, period_date, projection_start, projection_end, reporting_date, mp_year, mp_month, first_period_date, last_period_date
- Product / crop: product, item (FAO), Commodity, cpcv2_description, product_name, cm_name
- Admin/geography: admin_1, admin_2, admin_0, geographic_unit_name, fewsnet_region, admin_region, agroecological_zone
- Scenario: scenario_name

Query patterns (use these so the result directly answers the question):
- "Past decade" / "over the past N years" -> add WHERE planting_year >= (e.g. 2014) OR harvest_year BETWEEN ... OR year >= ... so only recent data is returned.
- "Which regions" / "which countries" / "which districts" -> GROUP BY the region column (country, admin_1, geographic_unit_name, etc.) and return one row per region; do not use SELECT * LIMIT 10.
- "Most significant changes" / "biggest changes" / "largest increase" -> compute a change metric (e.g. MAX(yield) - MIN(yield), or (yield in latest year - yield in earliest year)), GROUP BY region, ORDER BY that change DESC, LIMIT 10 or 20.
- "Compare" / "trends over time" -> GROUP BY region and year (or period), optionally aggregate (AVG(yield), SUM(production)), ORDER BY year/period.
- Always filter time when the question mentions a period; always aggregate and order when the question asks for "which" or "most".

Table hints (bronze dataset only — use only tables that appear in the Schema section):
- Yield/crop production: yield_raw_data (country, product, season_name, planting_year, harvest_year, area, production, yield)
- Food security / IPC: fews_net_food_security_master (country, geographic_unit_name, scenario_name, projection_start/end, ipc_phase_value)
- FAO: fao_rfn, fao_rl, fao_rp, fao_tcl, fao_ti, fao_fbs, fao_qcl (area/country_name, item, year, value)
- Cropland: cropland_area_summary_2019_africa (agroecological_zone and related columns per schema)
- GDP / development: africa_gdp_ppp, africa_Human_development_index (country, year)
"""

# Forbidden SQL tokens (case-insensitive) for safety
_FORBIDDEN_SQL = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|MERGE|TRUNCATE|ALTER|GRANT|REVOKE|EXEC|EXECUTE|CALL)\b",
    re.IGNORECASE,
)


def _validate_sql(sql: str, allowed_dataset_ids: set[str], default_limit: int) -> str | None:
    """
    Ensure SQL is a safe SELECT-only query over allowed datasets. Returns cleaned SQL or None.
    """
    normalized = " ".join(sql.split()).strip()
    if not normalized.upper().startswith("SELECT"):
        return None
    if _FORBIDDEN_SQL.search(normalized):
        return None
    # Ensure referenced datasets are in the allowed set (e.g. bronze only for RAG)
    if allowed_dataset_ids:
        allowed_lower = {a.lower() for a in allowed_dataset_ids}
        for part in re.findall(r"`?[\w.]+`?", normalized):
            part = part.strip("`")
            if "." in part:
                segments = part.split(".")
                # dataset.table or project.dataset.table
                ds = segments[-2].lower()
                if ds not in allowed_lower:
                    return None
    if "LIMIT" not in normalized.upper():
        normalized = f"{normalized.rstrip(';')} LIMIT {default_limit}"
    return normalized


class BQRetriever(BaseRetriever):
    """
    Retrieve context by querying BigQuery. Uses the bronze dataset only (BQ_DATASET_BRONZE).
    Uses Llama 3.1 (HF) for NL-to-SQL when no explicit sql is provided.
    """

    def __init__(
        self,
        project_id: str | None = None,
        max_rows: int = 100,
        nl2sql_enabled: bool | None = None,
    ):
        _load_dotenv()
        self.project_id = (project_id or os.environ.get("BQ_PROJECT", "")).strip()
        self.datasets_config = _get_datasets_config()
        self.max_rows = max_rows
        if nl2sql_enabled is not None:
            self.nl2sql_enabled = nl2sql_enabled
        else:
            self.nl2sql_enabled = os.environ.get("RAG_BQ_NL2SQL_ENABLED", "1").strip().lower() in ("1", "true", "on")
        self._client = None
        self._schema_cache: str | None = None

    def _get_client(self):
        if self._client is None:
            from google.cloud import bigquery
            self._client = bigquery.Client(project=self.project_id)
        return self._client

    def _get_schema(self) -> str:
        """Build a compact schema summary for configured datasets (bronze only); cached."""
        if self._schema_cache is not None:
            return self._schema_cache
        try:
            from google.cloud.bigquery import DatasetReference, TableReference
            client = self._get_client()
            lines = []
            for layer, dataset_id in self.datasets_config.items():
                if not dataset_id:
                    continue
                try:
                    dataset_ref = DatasetReference(self.project_id, dataset_id)
                    table_list = list(client.list_tables(dataset_ref))
                except Exception:
                    lines.append(f"[{layer}] dataset = {dataset_id}. (tables not listed)")
                    continue
                table_desc = []
                for t in table_list[:50]:  # cap tables per dataset
                    try:
                        table_ref = TableReference(dataset_ref, t.table_id)
                        table = client.get_table(table_ref)
                        cols = ", ".join(f"{f.name} {f.field_type}" for f in (table.schema or [])[:20])
                        table_desc.append(f"{t.table_id} ({cols})")
                    except Exception:
                        table_desc.append(t.table_id)
                lines.append(f"[{layer}] dataset = {dataset_id}. Tables: " + "; ".join(table_desc))
            self._schema_cache = "\n".join(lines) if lines else "[No schema]"
        except Exception:
            self._schema_cache = "[Schema unavailable]"
        return self._schema_cache

    def _nl_to_sql(self, question: str, table_hints: list[str] | None = None) -> str:
        """Generate a single BigQuery SELECT from the user question and current schema."""
        schema_text = self._get_schema()
        hints_block = ""
        if table_hints:
            cleaned = [str(h).strip() for h in table_hints if str(h).strip()]
            if cleaned:
                hints_block = (
                    "\n\nPrioritized table descriptions from the knowledge base (prefer these tables/columns when they match the question):\n"
                    + "\n".join(f"- {h[:800]}" for h in cleaned[:10])
                )
        system = (
            "You are a BigQuery expert for OpenTrace agricultural and food-security data in the bronze dataset only. "
            "Your job is to output exactly one valid BigQuery SQL SELECT that returns the most relevant rows. "
            "Rules: "
            "1) Use ONLY tables and columns that appear in the Schema section below (bronze dataset). Do not reference silver, gold, or landing datasets. "
            "Use full names: `project.dataset.table` or dataset.table. "
            "2) ALWAYS add WHERE clauses when the question mentions a specific value: "
            "   - Country (e.g. Kenya, Uganda) -> WHERE country = 'Kenya' or country_name = 'Kenya' or area = 'Kenya' (match schema). "
            "   - Season or year -> WHERE season_name = '...' OR planting_year = 2023 OR year = 2023 (use the column that exists). "
            "   - Product or crop (e.g. maize, rice) -> WHERE product = 'maize' OR item = 'Maize' OR Commodity = '...' (match schema). "
            "   - Region/district -> WHERE admin_1 = '...' OR admin_2 = '...' OR geographic_unit_name = '...' OR agroecological_zone = '...'. "
            "3) Use the Filter and table hints below to pick the right columns and tables; every table must exist in the Schema section. "
            "4) For 'which regions', 'most significant changes', 'compare', 'over time': ALWAYS add a time filter (e.g. planting_year >= 2014 for past decade), GROUP BY region, compute change or aggregate (e.g. MAX(yield)-MIN(yield)), ORDER BY that metric DESC, then LIMIT. Do NOT use SELECT * LIMIT 10 for such questions. "
            "5) Output only the SQL, no explanation, no markdown, no code fence."
        )
        user = (
            f"Filter and table hints (use these for WHERE and table choice):\n{_SCHEMA_FILTER_GUIDE}"
            f"{hints_block}\n\n"
            f"Schema (available tables and columns):\n{schema_text}\n\n"
            f"Question: {question}"
        )
        prompt = (
            f"<|begin_of_text|><|start_header_id|>system<|end_header_id|>\n{system}\n"
            f"<|eot_id|><|start_header_id|>user<|end_header_id|>\n{user}\n<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n"
        )
        raw = _call_llama_for_sql(prompt)
        if not raw:
            return ""
        # Strip markdown code block if present
        if "```" in raw:
            for block in re.findall(r"```(?:\w+)?\s*([\s\S]*?)```", raw):
                if block.strip().upper().startswith("SELECT"):
                    return block.strip()
            raw = re.sub(r"```[\s\S]*?```", "", raw).strip()
        return raw.strip()

    def _fallback_sql(self, question: str) -> str:
        """When NL-to-SQL fails (e.g. HF 410), build a sensible query from question keywords so results are still useful."""
        q = (question or "").lower()
        proj = self.project_id
        bronze_ds = self.datasets_config.get("bronze", "").strip()
        if not bronze_ds:
            return ""
        # Template: regions + past decade + "most significant changes" -> aggregated by country with yield change (bronze yield_raw_data)
        if any(x in q for x in ("which region", "which country", "which district", "most significant change", "past decade", "over the past")) and any(x in q for x in ("yield", "productivity", "crop", "production")):
            return (
                f"SELECT country, "
                f"MAX(yield) - MIN(yield) AS yield_change, "
                f"ROUND(AVG(yield), 2) AS avg_yield, "
                f"COUNT(*) AS n_obs "
                f"FROM `{proj}.{bronze_ds}.yield_raw_data` "
                f"WHERE planting_year >= 2014 AND yield IS NOT NULL "
                f"GROUP BY country "
                f"ORDER BY yield_change DESC "
                f"LIMIT {min(20, self.max_rows)}"
            )
        # Generic: first available table in configured datasets, LIMIT only
        try:
            from google.cloud.bigquery import DatasetReference
            for _layer, ds_id in self.datasets_config.items():
                if not ds_id:
                    continue
                try:
                    dataset_ref = DatasetReference(proj, ds_id)
                    table_list = list(self._get_client().list_tables(dataset_ref))
                    if table_list:
                        t = table_list[0]
                        return f"SELECT * FROM `{proj}.{ds_id}.{t.table_id}` LIMIT {min(10, self.max_rows)}"
                except Exception:
                    continue
        except Exception:
            pass
        return ""

    def retrieve(self, query: str, top_k: int = 10, **kwargs: Any) -> list[dict[str, Any]]:
        """
        Run a BQ query and return rows as context items.
        If kwargs["sql"] is provided, use it (after validation). Otherwise use NL-to-SQL over the bronze dataset.
        When NL-to-SQL returns nothing (e.g. LLM 410), a fallback query is used so BQ access can still be verified.
        """
        if not self.project_id:
            return []
        client = self._get_client()
        sql = kwargs.get("sql")
        table_hints = kwargs.get("table_hints")
        hint_list: list[str] | None = None
        if isinstance(table_hints, list) and table_hints:
            hint_list = [str(x) for x in table_hints if str(x).strip()]
        if not sql and self.nl2sql_enabled:
            sql = self._nl_to_sql(query, table_hints=hint_list)
        if not sql:
            sql = self._fallback_sql(query)
        if not sql:
            return []
        allowed = set(self.datasets_config.values())
        validated = _validate_sql(sql, allowed, self.max_rows)
        if validated is None:
            return []
        try:
            job = client.query(validated)
            rows = list(job.result())
        except Exception:
            return []
        items = []
        for row in rows[: top_k or self.max_rows]:
            d = dict(row)
            items.append({
                "content": str(d),
                "source": "bigquery",
                "metadata": {**d, "sql": validated},
            })
        return items
