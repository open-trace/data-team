#!/usr/bin/env python3
"""
Query BigQuery for Table Relationships

This script queries BigQuery to discover actual data relationships by:
1. Querying INFORMATION_SCHEMA for table metadata
2. Analyzing field value distributions
3. Checking referential integrity between tables
4. Identifying cardinality (1:1, 1:N, N:M)

Requires: Google Cloud credentials and BigQuery access
"""

import os
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from collections import defaultdict
from typing import Dict, List, Tuple
import json


class BigQueryRelationshipAnalyzer:
    """Analyze table relationships in BigQuery."""
    
    def __init__(self, project_id: str, dataset_id: str, credentials_path: str = None):
        """Initialize BigQuery client."""
        self.project_id = project_id
        self.dataset_id = dataset_id
        
        # Initialize BigQuery client
        if credentials_path and os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            self.client = bigquery.Client(credentials=credentials, project=project_id)
        else:
            # Use default credentials
            self.client = bigquery.Client(project=project_id)
        
        self.tables = []
        self.table_schemas = {}
        self.relationships = []
    
    def list_tables(self):
        """List all tables in the dataset."""
        print(f"Listing tables in {self.project_id}.{self.dataset_id}...")
        
        query = f"""
        SELECT 
            table_name,
            table_type,
            TIMESTAMP_MILLIS(creation_time) as created,
            row_count,
            size_bytes
        FROM `{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.TABLES`
        WHERE table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        
        try:
            df = self.client.query(query).to_dataframe()
            self.tables = df['table_name'].tolist()
            print(f"✓ Found {len(self.tables)} tables")
            return df
        except Exception as e:
            print(f"✗ Error listing tables: {e}")
            return pd.DataFrame()
    
    def get_table_schema(self, table_name: str):
        """Get schema information for a table."""
        query = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            is_partitioning_column
        FROM `{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        
        try:
            df = self.client.query(query).to_dataframe()
            self.table_schemas[table_name] = df
            return df
        except Exception as e:
            print(f"✗ Error getting schema for {table_name}: {e}")
            return pd.DataFrame()
    
    def check_referential_integrity(self, source_table: str, source_field: str, 
                                   target_table: str, target_field: str, 
                                   sample_size: int = 1000):
        """
        Check if values in source_field exist in target_field.
        Returns percentage of matching values.
        """
        query = f"""
        WITH source_sample AS (
            SELECT DISTINCT {source_field} as value
            FROM `{self.project_id}.{self.dataset_id}.{source_table}`
            WHERE {source_field} IS NOT NULL
            LIMIT {sample_size}
        ),
        target_values AS (
            SELECT DISTINCT {target_field} as value
            FROM `{self.project_id}.{self.dataset_id}.{target_table}`
            WHERE {target_field} IS NOT NULL
        ),
        matched AS (
            SELECT COUNT(*) as match_count
            FROM source_sample s
            INNER JOIN target_values t ON s.value = t.value
        ),
        total AS (
            SELECT COUNT(*) as total_count
            FROM source_sample
        )
        SELECT 
            m.match_count,
            t.total_count,
            SAFE_DIVIDE(m.match_count, t.total_count) * 100 as match_percentage
        FROM matched m, total t
        """
        
        try:
            result = self.client.query(query).to_dataframe()
            if not result.empty:
                return result.iloc[0]['match_percentage']
            return 0.0
        except Exception as e:
            print(f"✗ Error checking integrity {source_table}.{source_field} -> {target_table}.{target_field}: {e}")
            return 0.0
    
    def analyze_cardinality(self, source_table: str, source_field: str,
                           target_table: str, target_field: str):
        """
        Analyze the cardinality of a relationship.
        Returns: '1:1', '1:N', 'N:1', or 'N:M'
        """
        # Check uniqueness in source
        query_source = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT {source_field}) as distinct_values
        FROM `{self.project_id}.{self.dataset_id}.{source_table}`
        WHERE {source_field} IS NOT NULL
        """
        
        # Check uniqueness in target
        query_target = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT {target_field}) as distinct_values
        FROM `{self.project_id}.{self.dataset_id}.{target_table}`
        WHERE {target_field} IS NOT NULL
        """
        
        try:
            source_result = self.client.query(query_source).to_dataframe().iloc[0]
            target_result = self.client.query(query_target).to_dataframe().iloc[0]
            
            source_unique = source_result['total_rows'] == source_result['distinct_values']
            target_unique = target_result['total_rows'] == target_result['distinct_values']
            
            if source_unique and target_unique:
                return '1:1'
            elif source_unique and not target_unique:
                return '1:N'
            elif not source_unique and target_unique:
                return 'N:1'
            else:
                return 'N:M'
        except Exception as e:
            print(f"✗ Error analyzing cardinality: {e}")
            return 'unknown'
    
    def discover_relationships_from_csv(self, csv_file: str, 
                                       integrity_threshold: float = 80.0,
                                       max_relationships: int = 100):
        """
        Validate relationships from CSV file using BigQuery data.
        Only validates high-confidence relationships due to query costs.
        """
        print(f"\nValidating relationships from {csv_file}...")
        print(f"Integrity threshold: {integrity_threshold}%")
        print(f"Max relationships to validate: {max_relationships}")
        
        # Load discovered relationships
        df_relationships = pd.read_csv(csv_file)
        
        # Filter to high-confidence relationships only
        high_confidence = df_relationships[df_relationships['confidence'] == 'high'].head(max_relationships)
        
        print(f"Validating {len(high_confidence)} high-confidence relationships...")
        
        validated_relationships = []
        
        for idx, row in high_confidence.iterrows():
            source_table = row['source_table']
            source_field = row['source_field']
            target_table = row['target_table']
            target_field = row['target_field']
            
            print(f"\n[{idx+1}/{len(high_confidence)}] Checking: {source_table}.{source_field} -> {target_table}.{target_field}")
            
            # Check if tables exist
            if source_table not in self.tables or target_table not in self.tables:
                print(f"  ⚠ Skipping: Table not found in BigQuery")
                continue
            
            # Check referential integrity
            match_pct = self.check_referential_integrity(
                source_table, source_field,
                target_table, target_field
            )
            
            print(f"  Match percentage: {match_pct:.1f}%")
            
            if match_pct >= integrity_threshold:
                # Analyze cardinality
                cardinality = self.analyze_cardinality(
                    source_table, source_field,
                    target_table, target_field
                )
                
                print(f"  ✓ Validated! Cardinality: {cardinality}")
                
                validated_relationships.append({
                    'source_table': source_table,
                    'source_field': source_field,
                    'target_table': target_table,
                    'target_field': target_field,
                    'cardinality': cardinality,
                    'match_percentage': match_pct,
                    'validation_status': 'validated',
                    'original_confidence': row['confidence']
                })
            else:
                print(f"  ✗ Failed validation (below threshold)")
        
        self.relationships = validated_relationships
        print(f"\n✓ Validated {len(validated_relationships)} relationships")
        
        return validated_relationships
    
    def generate_report(self, output_file: str):
        """Generate validated relationships report."""
        print(f"\nGenerating report: {output_file}")
        
        if self.relationships:
            df = pd.DataFrame(self.relationships)
            df = df.sort_values(by=['match_percentage', 'source_table'], ascending=[False, True])
            df.to_csv(output_file, index=False)
            print(f"✓ Saved {len(df)} validated relationships to {output_file}")
        else:
            print("⚠ No validated relationships to save")
    
    def generate_summary(self):
        """Generate summary statistics."""
        print("\n" + "=" * 80)
        print("BIGQUERY RELATIONSHIP VALIDATION SUMMARY")
        print("=" * 80)
        
        if self.relationships:
            df = pd.DataFrame(self.relationships)
            
            print(f"\nTotal validated relationships: {len(df)}")
            print(f"Average match percentage: {df['match_percentage'].mean():.1f}%")
            
            print("\nRelationships by cardinality:")
            print(df['cardinality'].value_counts().to_string())
            
            print("\nTop 10 strongest relationships (by match %):")
            top_10 = df.nlargest(10, 'match_percentage')[
                ['source_table', 'source_field', 'target_table', 'target_field', 'match_percentage', 'cardinality']
            ]
            print(top_10.to_string(index=False))
        else:
            print("\nNo relationships validated")


def main():
    """Main execution function."""
    print("=" * 80)
    print("BIGQUERY RELATIONSHIP VALIDATOR")
    print("=" * 80)
    
    # Configuration
    PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'your-project-id')
    DATASET_ID = os.getenv('BQ_DATASET_ID', 'landing')
    CREDENTIALS_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', None)
    
    print(f"\nConfiguration:")
    print(f"  Project ID: {PROJECT_ID}")
    print(f"  Dataset ID: {DATASET_ID}")
    print(f"  Credentials: {CREDENTIALS_PATH or 'Using default credentials'}")
    
    # Check if we should run (requires valid config)
    if PROJECT_ID == 'your-project-id':
        print("\n⚠ WARNING: Please set GCP_PROJECT_ID environment variable")
        print("Example: export GCP_PROJECT_ID='your-actual-project-id'")
        print("\nSkipping BigQuery validation for now.")
        print("You can run this script later after setting up credentials.")
        return
    
    try:
        # Initialize analyzer
        analyzer = BigQueryRelationshipAnalyzer(PROJECT_ID, DATASET_ID, CREDENTIALS_PATH)
        
        # List tables
        tables_df = analyzer.list_tables()
        
        if tables_df.empty:
            print("\n⚠ No tables found. Please check your configuration.")
            return
        
        # Validate relationships from CSV
        input_csv = 'data-eng/docs/table_relationships_discovered.csv'
        output_csv = 'data-eng/docs/table_relationships_validated.csv'
        
        validated = analyzer.discover_relationships_from_csv(
            input_csv,
            integrity_threshold=80.0,
            max_relationships=50  # Limit to avoid high query costs
        )
        
        # Generate report
        analyzer.generate_report(output_csv)
        analyzer.generate_summary()
        
        print("\n" + "=" * 80)
        print("VALIDATION COMPLETE")
        print("=" * 80)
        print(f"\n📁 Output file: {output_csv}")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        print("\nIf you don't have BigQuery access set up yet, you can skip this step")
        print("and proceed with the relationships discovered from field name analysis.")


if __name__ == '__main__':
    main()
