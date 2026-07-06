#!/usr/bin/env python3
"""
Analyze Table Relationships from Excel Documentation

This script reads the agricultural_indicator_schema_catalog_updated.xlsx file
and identifies potential table relationships by analyzing field names and patterns.

It identifies:
- Primary keys (id, *_id, *_key fields)
- Foreign keys (fields that match primary keys in other tables)
- Common relationship patterns (country_code, area_code, etc.)
- Potential relationships between tables

Output: CSV file with discovered relationships
"""

import pandas as pd
import re
from collections import defaultdict
from typing import Dict, List, Tuple, Set


class TableRelationshipAnalyzer:
    """Analyze table relationships from field documentation."""
    
    def __init__(self, excel_file: str):
        """Initialize analyzer with Excel file path."""
        self.excel_file = excel_file
        self.df = None
        self.tables = {}
        self.primary_keys = defaultdict(list)
        self.foreign_keys = defaultdict(list)
        self.relationships = []
        
    def load_data(self):
        """Load data from Excel file."""
        print(f"Loading data from {self.excel_file}...")
        self.df = pd.read_excel(self.excel_file, sheet_name='Entity_mapping')
        print(f"Loaded {len(self.df)} rows")
        
        # Group by table
        for table_name in self.df['table_name'].unique():
            if pd.notna(table_name):
                table_df = self.df[self.df['table_name'] == table_name]
                self.tables[table_name] = table_df
        
        print(f"Found {len(self.tables)} unique tables")
    
    def identify_primary_keys(self):
        """Identify primary key fields in each table."""
        print("\nIdentifying primary keys...")
        
        pk_patterns = [
            r'^id$',
            r'^.*_id$',
            r'^.*_key$',
            r'^key$',
            r'^.*_code$',  # Sometimes codes are primary keys
        ]
        
        for table_name, table_df in self.tables.items():
            for _, row in table_df.iterrows():
                field_name = str(row['entity name']).lower().strip()
                
                # Check if field matches primary key patterns
                for pattern in pk_patterns:
                    if re.match(pattern, field_name):
                        # Prioritize simple 'id' fields
                        if field_name == 'id':
                            self.primary_keys[table_name].insert(0, {
                                'field': row['entity name'],
                                'data_type': row['data_type'],
                                'confidence': 'high'
                            })
                        else:
                            self.primary_keys[table_name].append({
                                'field': row['entity name'],
                                'data_type': row['data_type'],
                                'confidence': 'medium'
                            })
                        break
        
        print(f"Identified primary keys in {len(self.primary_keys)} tables")
    
    def identify_foreign_keys(self):
        """Identify foreign key fields by matching field names across tables."""
        print("\nIdentifying foreign keys...")
        
        # Common foreign key patterns
        fk_patterns = {
            'country': ['country_code', 'country', 'country_name', 'alpha_3_code', 'alpha_2_code'],
            'area': ['area_code', 'area', 'area_name'],
            'region': ['region', 'region_code', 'fewsnet_region', 'geographic_group'],
            'source': ['source', 'source_code', 'data_source', 'datasourceorganization', 'datasourcedocument'],
            'series': ['dataseries', 'series_id', 'series_code'],
            'market': ['market', 'market_id', 'market_name'],
            'product': ['product', 'product_code', 'product_name', 'item', 'item_code'],
            'element': ['element', 'element_code'],
            'domain': ['domain', 'domain_code'],
        }
        
        for table_name, table_df in self.tables.items():
            for _, row in table_df.iterrows():
                field_name = str(row['entity name']).lower().strip()
                
                # Check against foreign key patterns
                for fk_type, patterns in fk_patterns.items():
                    if field_name in patterns:
                        self.foreign_keys[table_name].append({
                            'field': row['entity name'],
                            'data_type': row['data_type'],
                            'fk_type': fk_type,
                            'confidence': 'high'
                        })
                        break
        
        print(f"Identified foreign keys in {len(self.foreign_keys)} tables")
    
    def discover_relationships(self):
        """Discover relationships between tables based on field name matching."""
        print("\nDiscovering relationships...")
        
        relationship_count = 0
        
        # For each table with foreign keys
        for source_table, fks in self.foreign_keys.items():
            for fk in fks:
                fk_field = fk['field'].lower().strip()
                
                # Look for matching primary keys in other tables
                for target_table, pks in self.primary_keys.items():
                    if source_table == target_table:
                        continue  # Skip self-references for now
                    
                    for pk in pks:
                        pk_field = pk['field'].lower().strip()
                        
                        # Check for exact match
                        if fk_field == pk_field:
                            self.relationships.append({
                                'source_table': source_table,
                                'source_field': fk['field'],
                                'target_table': target_table,
                                'target_field': pk['field'],
                                'relationship_type': 'many-to-one',
                                'match_type': 'exact',
                                'confidence': 'high'
                            })
                            relationship_count += 1
                        
                        # Check for pattern match (e.g., country_code -> country)
                        elif fk_field.replace('_code', '').replace('_name', '').replace('_id', '') == \
                             pk_field.replace('_code', '').replace('_name', '').replace('_id', ''):
                            self.relationships.append({
                                'source_table': source_table,
                                'source_field': fk['field'],
                                'target_table': target_table,
                                'target_field': pk['field'],
                                'relationship_type': 'many-to-one',
                                'match_type': 'pattern',
                                'confidence': 'medium'
                            })
                            relationship_count += 1
        
        print(f"Discovered {relationship_count} potential relationships")
    
    def infer_common_relationships(self):
        """Infer common relationships based on domain knowledge."""
        print("\nInferring common relationships...")
        
        # Common relationship patterns in agricultural data
        common_patterns = [
            # FAO tables often relate through country, area, item, element
            {
                'source_pattern': r'fao_.*',
                'target_pattern': r'fao_.*',
                'link_fields': ['country_code', 'area_code', 'item_code', 'element_code'],
                'confidence': 'medium'
            },
            # FEWS NET tables relate through fnid, country, geographic_unit
            {
                'source_pattern': r'FEWS_NET_.*',
                'target_pattern': r'FEWS_NET_.*',
                'link_fields': ['fnid', 'country_code', 'geographic_unit'],
                'confidence': 'medium'
            },
            # ILRI tables may relate through country, region
            {
                'source_pattern': r'ilri_.*',
                'target_pattern': r'ilri_.*',
                'link_fields': ['country', 'region'],
                'confidence': 'low'
            },
        ]
        
        inferred_count = 0
        
        for pattern in common_patterns:
            source_tables = [t for t in self.tables.keys() if re.match(pattern['source_pattern'], t)]
            target_tables = [t for t in self.tables.keys() if re.match(pattern['target_pattern'], t)]
            
            for source_table in source_tables:
                source_fields = self.tables[source_table]['entity name'].str.lower().tolist()
                
                for target_table in target_tables:
                    if source_table == target_table:
                        continue
                    
                    target_fields = self.tables[target_table]['entity name'].str.lower().tolist()
                    
                    # Check for common link fields
                    for link_field in pattern['link_fields']:
                        if link_field in source_fields and link_field in target_fields:
                            # Check if relationship already exists
                            exists = any(
                                r['source_table'] == source_table and
                                r['target_table'] == target_table and
                                r['source_field'].lower() == link_field
                                for r in self.relationships
                            )
                            
                            if not exists:
                                self.relationships.append({
                                    'source_table': source_table,
                                    'source_field': link_field,
                                    'target_table': target_table,
                                    'target_field': link_field,
                                    'relationship_type': 'many-to-many',
                                    'match_type': 'inferred',
                                    'confidence': pattern['confidence']
                                })
                                inferred_count += 1
        
        print(f"Inferred {inferred_count} additional relationships")
    
    def generate_report(self, output_file: str):
        """Generate relationship report as CSV."""
        print(f"\nGenerating report: {output_file}")
        
        # Convert relationships to DataFrame
        if self.relationships:
            df_relationships = pd.DataFrame(self.relationships)
            
            # Sort by confidence and source table
            df_relationships = df_relationships.sort_values(
                by=['confidence', 'source_table', 'target_table'],
                ascending=[False, True, True]
            )
            
            # Save to CSV
            df_relationships.to_csv(output_file, index=False)
            print(f"✓ Saved {len(df_relationships)} relationships to {output_file}")
        else:
            print("⚠ No relationships found")
    
    def generate_summary(self):
        """Generate summary statistics."""
        print("\n" + "=" * 80)
        print("RELATIONSHIP ANALYSIS SUMMARY")
        print("=" * 80)
        
        print(f"\nTables analyzed: {len(self.tables)}")
        print(f"Tables with primary keys: {len(self.primary_keys)}")
        print(f"Tables with foreign keys: {len(self.foreign_keys)}")
        print(f"Total relationships discovered: {len(self.relationships)}")
        
        # Breakdown by confidence
        if self.relationships:
            df_rel = pd.DataFrame(self.relationships)
            print("\nRelationships by confidence:")
            print(df_rel['confidence'].value_counts().to_string())
            
            print("\nRelationships by match type:")
            print(df_rel['match_type'].value_counts().to_string())
            
            print("\nTop 10 most connected tables (as source):")
            print(df_rel['source_table'].value_counts().head(10).to_string())
    
    def run(self, output_file: str):
        """Run the complete analysis."""
        print("=" * 80)
        print("TABLE RELATIONSHIP ANALYZER")
        print("=" * 80)
        
        self.load_data()
        self.identify_primary_keys()
        self.identify_foreign_keys()
        self.discover_relationships()
        self.infer_common_relationships()
        self.generate_report(output_file)
        self.generate_summary()
        
        print("\n" + "=" * 80)
        print("ANALYSIS COMPLETE")
        print("=" * 80)


def main():
    """Main execution function."""
    input_file = 'data-eng/data/agricultural_indicator_schema_catalog_updated.xlsx'
    output_file = 'data-eng/docs/table_relationships_discovered.csv'
    
    analyzer = TableRelationshipAnalyzer(input_file)
    analyzer.run(output_file)
    
    print(f"\n📁 Output file: {output_file}")
    print("📝 Next step: Review the relationships and refine them manually")


if __name__ == '__main__':
    main()
