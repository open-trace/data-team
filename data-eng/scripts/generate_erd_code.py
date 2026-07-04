#!/usr/bin/env python3
"""
Generate ERD Diagram Code

This script generates ERD (Entity-Relationship Diagram) code in multiple formats:
1. DBML (Database Markup Language) for dbdiagram.io
2. Mermaid diagram code
3. PlantUML code

Input: CSV file with discovered relationships
Output: ERD code files in multiple formats
"""

import pandas as pd
from collections import defaultdict
from typing import Dict, List, Set
import re


class ERDCodeGenerator:
    """Generate ERD code in multiple formats."""
    
    def __init__(self, relationships_csv: str, excel_file: str):
        """Initialize with relationships CSV and Excel documentation."""
        self.relationships_csv = relationships_csv
        self.excel_file = excel_file
        self.df_relationships = None
        self.df_fields = None
        self.tables = {}
        self.table_categories = {}
        
    def load_data(self):
        """Load relationships and field documentation."""
        print("Loading data...")
        
        # Load relationships
        self.df_relationships = pd.read_csv(self.relationships_csv)
        print(f"✓ Loaded {len(self.df_relationships)} relationships")
        
        # Load field documentation
        self.df_fields = pd.read_excel(self.excel_file, sheet_name='Entity_mapping')
        print(f"✓ Loaded {len(self.df_fields)} field definitions")
        
        # Group fields by table
        for table_name in self.df_fields['table_name'].unique():
            if pd.notna(table_name):
                table_df = self.df_fields[self.df_fields['table_name'] == table_name]
                self.tables[table_name] = table_df
        
        print(f"✓ Found {len(self.tables)} unique tables")
    
    def categorize_tables(self):
        """Categorize tables by source and type."""
        print("\nCategorizing tables...")
        
        for table_name in self.tables.keys():
            table_lower = table_name.lower()
            
            # Determine source category
            if table_lower.startswith('fao_'):
                category = 'FAO'
            elif table_lower.startswith('fews_net_'):
                category = 'FEWS NET'
            elif table_lower.startswith('ilri_'):
                category = 'ILRI'
            elif table_lower.startswith('wfp_'):
                category = 'WFP'
            elif 'gbif' in table_lower:
                category = 'GBIF'
            elif 'isric' in table_lower or 'soil' in table_lower:
                category = 'ISRIC'
            elif 'climate' in table_lower or 'weather' in table_lower:
                category = 'Climate'
            else:
                category = 'Other'
            
            # Determine if fact or dimension
            table_df = self.tables[table_name]
            
            # Check if 'placement' or 'placemant' column exists (typo in Excel)
            placement_col = None
            if 'placement' in table_df.columns:
                placement_col = 'placement'
            elif 'placemant' in table_df.columns:
                placement_col = 'placemant'
            
            if placement_col:
                placement_values = table_df[placement_col].value_counts()
                if 'Fact' in placement_values.index and placement_values.get('Fact', 0) > len(table_df) * 0.3:
                    table_type = 'Fact'
                else:
                    table_type = 'Dimension'
            else:
                # Default to Dimension if no placement column
                table_type = 'Dimension'
            
            self.table_categories[table_name] = {
                'source': category,
                'type': table_type
            }
        
        print(f"✓ Categorized {len(self.table_categories)} tables")
    
    def sanitize_dbml_identifier(self, name: str) -> str:
        """Sanitize table/field names to be valid DBML identifiers."""
        # Replace spaces, hyphens, and other special chars with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        
        # Ensure doesn't start with a digit
        if sanitized and sanitized[0].isdigit():
            sanitized = 'tbl_' + sanitized
        
        # Remove consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        
        return sanitized
    
    def generate_dbml(self, output_file: str, max_tables: int = 50):
        """Generate DBML code for dbdiagram.io."""
        print(f"\nGenerating DBML code (max {max_tables} tables)...")
        
        dbml_lines = []
        dbml_lines.append("// Database Schema ERD")
        dbml_lines.append("// Generated from agricultural_indicator_schema_catalog")
        dbml_lines.append("// Use at https://dbdiagram.io\n")
        dbml_lines.append("// Note: Table names have been sanitized for DBML compatibility")
        dbml_lines.append("// Original names are preserved in comments\n")
        
        # Get most connected tables
        table_connections = defaultdict(int)
        for _, row in self.df_relationships.iterrows():
            table_connections[row['source_table']] += 1
            table_connections[row['target_table']] += 1
        
        top_tables = sorted(table_connections.items(), key=lambda x: x[1], reverse=True)[:max_tables]
        selected_tables = {t[0] for t in top_tables}
        
        # Create mapping of original to sanitized names
        table_name_mapping = {}
        for table_name in selected_tables:
            sanitized = self.sanitize_dbml_identifier(table_name)
            table_name_mapping[table_name] = sanitized
        
        print(f"Selected {len(selected_tables)} most connected tables")
        
        # Generate table definitions
        for table_name in sorted(selected_tables):
            if table_name not in self.tables:
                continue
            
            table_df = self.tables[table_name]
            category = self.table_categories.get(table_name, {})
            
            sanitized_table_name = table_name_mapping[table_name]
            
            # Table header with note
            dbml_lines.append(f"Table {sanitized_table_name} {{")
            if sanitized_table_name != table_name:
                dbml_lines.append(f"  // Original name: {table_name}")
            dbml_lines.append(f"  // Source: {category.get('source', 'Unknown')}")
            dbml_lines.append(f"  // Type: {category.get('type', 'Unknown')}")
            
            # Add fields (limit to key fields for readability)
            key_fields = []
            for _, field in table_df.iterrows():
                field_name = str(field['entity name']).strip()
                field_name_lower = field_name.lower()
                
                # Include primary keys, foreign keys, and important fields
                if (field_name_lower == 'id' or 
                    field_name_lower.endswith('_id') or 
                    field_name_lower.endswith('_code') or
                    field_name_lower.endswith('_key') or
                    field_name_lower in ['country', 'country_code', 'area_code', 'date', 'year']):
                    
                    data_type = str(field['data_type']).upper()
                    # Map BigQuery types to DBML types
                    type_mapping = {
                        'STRING': 'varchar',
                        'INT64': 'integer',
                        'FLOAT64': 'float',
                        'DATE': 'date',
                        'TIMESTAMP': 'timestamp',
                        'BOOLEAN': 'boolean'
                    }
                    dbml_type = type_mapping.get(data_type, 'varchar')
                    
                    # Mark primary keys
                    pk_marker = ' [pk]' if field_name_lower == 'id' else ''
                    
                    dbml_lines.append(f"  {field_name} {dbml_type}{pk_marker}")
                    key_fields.append(field_name)
            
            # Add note about other fields
            total_fields = len(table_df)
            shown_fields = len(key_fields)
            if total_fields > shown_fields:
                dbml_lines.append(f"  // ... and {total_fields - shown_fields} more fields")
            
            dbml_lines.append("}\n")
        
        # Generate relationships
        dbml_lines.append("// Relationships")
        
        # Filter relationships to only include selected tables
        filtered_rels = self.df_relationships[
            (self.df_relationships['source_table'].isin(selected_tables)) &
            (self.df_relationships['target_table'].isin(selected_tables)) &
            (self.df_relationships['confidence'] == 'high')
        ].head(100)  # Limit relationships for readability
        
        for _, rel in filtered_rels.iterrows():
            source_table = rel['source_table']
            source_field = rel['source_field']
            target_table = rel['target_table']
            target_field = rel['target_field']
            
            # Use sanitized table names in relationships
            sanitized_source = table_name_mapping.get(source_table, source_table)
            sanitized_target = table_name_mapping.get(target_table, target_table)
            
            # DBML relationship syntax: Ref: table1.field > table2.field
            dbml_lines.append(f"Ref: {sanitized_source}.{source_field} > {sanitized_target}.{target_field}")
        
        # Write to file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(dbml_lines))
        
        print(f"✓ Generated DBML code: {output_file}")
        print(f"  Tables: {len(selected_tables)}")
        print(f"  Relationships: {len(filtered_rels)}")
    
    def generate_mermaid(self, output_file: str, max_tables: int = 30):
        """Generate Mermaid diagram code."""
        print(f"\nGenerating Mermaid code (max {max_tables} tables)...")
        
        mermaid_lines = []
        mermaid_lines.append("```mermaid")
        mermaid_lines.append("erDiagram")
        
        # Get most connected tables
        table_connections = defaultdict(int)
        for _, row in self.df_relationships.iterrows():
            table_connections[row['source_table']] += 1
            table_connections[row['target_table']] += 1
        
        top_tables = sorted(table_connections.items(), key=lambda x: x[1], reverse=True)[:max_tables]
        selected_tables = {t[0] for t in top_tables}
        
        # Generate table definitions
        for table_name in sorted(selected_tables):
            if table_name not in self.tables:
                continue
            
            table_df = self.tables[table_name]
            
            # Sanitize table name for Mermaid
            safe_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
            
            mermaid_lines.append(f"  {safe_table_name} {{")
            
            # Add key fields only
            for _, field in table_df.head(10).iterrows():  # Limit fields
                field_name = str(field['entity name']).strip()
                data_type = str(field['data_type'])
                safe_field_name = re.sub(r'[^a-zA-Z0-9_]', '_', field_name)
                mermaid_lines.append(f"    {data_type} {safe_field_name}")
            
            mermaid_lines.append("  }")
        
        # Generate relationships
        filtered_rels = self.df_relationships[
            (self.df_relationships['source_table'].isin(selected_tables)) &
            (self.df_relationships['target_table'].isin(selected_tables)) &
            (self.df_relationships['confidence'] == 'high')
        ].head(50)
        
        for _, rel in filtered_rels.iterrows():
            source = re.sub(r'[^a-zA-Z0-9_]', '_', rel['source_table'])
            target = re.sub(r'[^a-zA-Z0-9_]', '_', rel['target_table'])
            
            # Mermaid relationship syntax
            mermaid_lines.append(f"  {source} ||--o{{ {target} : has")
        
        mermaid_lines.append("```")
        
        # Write to file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(mermaid_lines))
        
        print(f"✓ Generated Mermaid code: {output_file}")
    
    def generate_summary_doc(self, output_file: str):
        """Generate a summary document of the ERD."""
        print(f"\nGenerating summary document...")
        
        lines = []
        lines.append("# Entity-Relationship Diagram (ERD) Summary\n")
        lines.append("## Overview\n")
        lines.append(f"- **Total Tables**: {len(self.tables)}")
        lines.append(f"- **Total Relationships**: {len(self.df_relationships)}")
        lines.append(f"- **Total Fields**: {len(self.df_fields)}\n")
        
        # Tables by category
        lines.append("## Tables by Source\n")
        source_counts = defaultdict(int)
        for cat in self.table_categories.values():
            source_counts[cat['source']] += 1
        
        for source, count in sorted(source_counts.items(), key=lambda x: x[1], reverse=True):
            lines.append(f"- **{source}**: {count} tables")
        
        lines.append("\n## Tables by Type\n")
        type_counts = defaultdict(int)
        for cat in self.table_categories.values():
            type_counts[cat['type']] += 1
        
        for table_type, count in sorted(type_counts.items()):
            lines.append(f"- **{table_type}**: {count} tables")
        
        # Relationship statistics
        lines.append("\n## Relationship Statistics\n")
        lines.append(f"- **High Confidence**: {len(self.df_relationships[self.df_relationships['confidence'] == 'high'])}")
        lines.append(f"- **Medium Confidence**: {len(self.df_relationships[self.df_relationships['confidence'] == 'medium'])}")
        lines.append(f"- **Low Confidence**: {len(self.df_relationships[self.df_relationships['confidence'] == 'low'])}")
        
        # Most connected tables
        lines.append("\n## Most Connected Tables\n")
        table_connections = defaultdict(int)
        for _, row in self.df_relationships.iterrows():
            table_connections[row['source_table']] += 1
            table_connections[row['target_table']] += 1
        
        top_10 = sorted(table_connections.items(), key=lambda x: x[1], reverse=True)[:10]
        for table, count in top_10:
            category = self.table_categories.get(table, {})
            lines.append(f"- **{table}** ({category.get('source', 'Unknown')}): {count} connections")
        
        # Write to file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        
        print(f"✓ Generated summary document: {output_file}")
    
    def run(self):
        """Run the complete ERD code generation."""
        print("=" * 80)
        print("ERD CODE GENERATOR")
        print("=" * 80)
        
        self.load_data()
        self.categorize_tables()
        
        # Generate different formats
        self.generate_dbml('data-eng/docs/ERD_diagram.dbml')
        self.generate_mermaid('data-eng/docs/ERD_diagram_mermaid.md')
        self.generate_summary_doc('data-eng/docs/ERD_summary.md')
        
        print("\n" + "=" * 80)
        print("ERD CODE GENERATION COMPLETE")
        print("=" * 80)
        print("\n📁 Output files:")
        print("  - data-eng/docs/ERD_diagram.dbml (use at https://dbdiagram.io)")
        print("  - data-eng/docs/ERD_diagram_mermaid.md (Mermaid format)")
        print("  - data-eng/docs/ERD_summary.md (Summary document)")
        print("\n📝 Next steps:")
        print("  1. Open https://dbdiagram.io")
        print("  2. Paste the contents of ERD_diagram.dbml")
        print("  3. Adjust layout and export as PNG/PDF")


def main():
    """Main execution function."""
    relationships_csv = 'data-eng/docs/table_relationships_discovered.csv'
    excel_file = 'data-eng/data/agricultural_indicator_schema_catalog_updated.xlsx'
    
    generator = ERDCodeGenerator(relationships_csv, excel_file)
    generator.run()


if __name__ == '__main__':
    main()
