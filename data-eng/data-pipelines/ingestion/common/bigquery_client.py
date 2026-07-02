"""BigQuery client utilities for data ingestion."""
import os
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
import yaml

try:
    from google.cloud import bigquery
    from google.cloud.exceptions import GoogleCloudError
except ImportError:
    bigquery = None
    GoogleCloudError = Exception


logger = logging.getLogger(__name__)


def load_bigquery_config() -> Dict:
    """Load BigQuery configuration from ingestion_sources.yaml.
    
    Returns:
        Dict with BigQuery configuration
    """
    config_path = Path(__file__).parents[3] / "config" / "ingestion_sources.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config.get('bigquery', {})


class BigQueryClient:
    """Wrapper for BigQuery operations with error handling and logging."""
    
    def __init__(self, project_id: Optional[str] = None, dataset_id: Optional[str] = None):
        """Initialize BigQuery client.
        
        Args:
            project_id: GCP project ID (defaults to config or env var)
            dataset_id: BigQuery dataset ID (defaults to config)
        """
        if bigquery is None:
            raise ImportError(
                "google-cloud-bigquery is required. Install: pip install google-cloud-bigquery"
            )
        
        config = load_bigquery_config()
        self.project_id = project_id or config.get('project_id') or os.getenv('BQ_PROJECT')
        self.dataset_id = dataset_id or config.get('landing_dataset', 'landing')
        self.location = config.get('location', 'europe-west3')
        
        # Set credentials from environment variable
        credentials_path = os.getenv(config.get('credentials_env_var', 'GOOGLE_APPLICATION_CREDENTIALS'))
        if credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        
        self.client = bigquery.Client(project=self.project_id, location=self.location)
        logger.info(f"Initialized BigQuery client for project: {self.project_id}, dataset: {self.dataset_id}")
    
    def table_exists(self, table_id: str) -> bool:
        """Check if a table exists in the dataset.
        
        Args:
            table_id: Table name
            
        Returns:
            True if table exists, False otherwise
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        try:
            self.client.get_table(table_ref)
            return True
        except Exception:
            return False
    
    def create_table(self, table_id: str, schema: List[bigquery.SchemaField]) -> None:
        """Create a new table with the given schema.
        
        Args:
            table_id: Table name
            schema: List of SchemaField objects
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        table = bigquery.Table(table_ref, schema=schema)
        
        try:
            self.client.create_table(table)
            logger.info(f"Created table: {table_ref}")
        except GoogleCloudError as e:
            logger.error(f"Failed to create table {table_ref}: {e}")
            raise
    
    def insert_rows(self, table_id: str, rows: List[Dict[str, Any]]) -> None:
        """Insert rows into a BigQuery table.
        
        Args:
            table_id: Table name
            rows: List of dictionaries representing rows
        """
        if not rows:
            logger.warning(f"No rows to insert into {table_id}")
            return
        
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        
        try:
            errors = self.client.insert_rows_json(table_ref, rows)
            if errors:
                logger.error(f"Errors inserting rows into {table_ref}: {errors}")
                raise Exception(f"BigQuery insert errors: {errors}")
            else:
                logger.info(f"Successfully inserted {len(rows)} rows into {table_ref}")
        except GoogleCloudError as e:
            logger.error(f"Failed to insert rows into {table_ref}: {e}")
            raise
    
    def load_from_dataframe(self, table_id: str, dataframe, write_disposition: str = "WRITE_APPEND") -> None:
        """Load data from a pandas DataFrame into BigQuery.
        
        Args:
            table_id: Table name
            dataframe: pandas DataFrame
            write_disposition: WRITE_APPEND, WRITE_TRUNCATE, or WRITE_EMPTY
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=True
        )
        
        try:
            job = self.client.load_table_from_dataframe(
                dataframe, table_ref, job_config=job_config
            )
            job.result()  # Wait for job to complete
            logger.info(f"Loaded {len(dataframe)} rows into {table_ref}")
        except GoogleCloudError as e:
            logger.error(f"Failed to load DataFrame into {table_ref}: {e}")
            raise
    
    def query(self, sql: str) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results.
        
        Args:
            sql: SQL query string
            
        Returns:
            List of dictionaries representing rows
        """
        try:
            query_job = self.client.query(sql)
            results = query_job.result()
            return [dict(row) for row in results]
        except GoogleCloudError as e:
            logger.error(f"Query failed: {e}")
            raise
    
    def get_table_schema(self, table_id: str) -> List[bigquery.SchemaField]:
        """Get the schema of an existing table.
        
        Args:
            table_id: Table name
            
        Returns:
            List of SchemaField objects
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        try:
            table = self.client.get_table(table_ref)
            return table.schema
        except GoogleCloudError as e:
            logger.error(f"Failed to get schema for {table_ref}: {e}")
            raise
    
    def delete_table(self, table_id: str) -> None:
        """Delete a table.
        
        Args:
            table_id: Table name
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        try:
            self.client.delete_table(table_ref)
            logger.info(f"Deleted table: {table_ref}")
        except GoogleCloudError as e:
            logger.error(f"Failed to delete table {table_ref}: {e}")
            raise
    
    def get_row_count(self, table_id: str) -> int:
        """Get the number of rows in a table.
        
        Args:
            table_id: Table name
            
        Returns:
            Number of rows
        """
        sql = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_id}.{table_id}`"
        results = self.query(sql)
        return results[0]['count'] if results else 0
