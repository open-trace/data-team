#!/usr/bin/env python3
"""
Generate intelligent field descriptions based on field names and patterns.
Fills in missing descriptions in the agricultural_indicator_schema_catalog.xlsx file.
"""

import pandas as pd
import re
from datetime import datetime


class FieldDescriptionGenerator:
    """Generate descriptions for data warehouse fields based on patterns and domain knowledge."""
    
    def __init__(self):
        """Initialize the generator with domain knowledge and patterns."""
        
        # Common field patterns and their descriptions
        self.common_patterns = {
            # IDs and Keys
            r'^id$': 'Unique identifier for the record',
            r'_id$': 'Identifier for {entity}',
            r'^.*_key$': 'Foreign key reference to {entity} dimension table',
            r'^key$': 'Primary key identifier',
            
            # Codes
            r'_code$': 'Standardized code for {entity}',
            r'^code$': 'Standardized classification code',
            r'alpha_\d_code': 'ISO {entity} code',
            
            # Names and Labels
            r'_name$': 'Human-readable name for {entity}',
            r'^name$': 'Name or label of the entity',
            
            # Geographic fields
            r'^country$': 'Country name where the data was collected or applies',
            r'^country_code$': 'ISO country code (alpha-2 or alpha-3 format)',
            r'^country_name$': 'Full name of the country',
            r'^admin_\d+$': 'Administrative division level {level} (e.g., province, district, sub-district)',
            r'^region$': 'Geographic region or area',
            r'^geographic': 'Geographic location or administrative unit',
            r'^latitude$': 'Latitude coordinate in decimal degrees (WGS84)',
            r'^longitude$': 'Longitude coordinate in decimal degrees (WGS84)',
            r'^location$': 'Geographic location or place name',
            
            # Temporal fields
            r'^year$': 'Year of observation or data collection',
            r'^month$': 'Month of observation (1-12)',
            r'^day$': 'Day of the month (1-31)',
            r'^date$': 'Date of observation or event',
            r'_date$': 'Date when {entity} occurred or was recorded',
            r'^period': 'Time period for the observation',
            r'^season': 'Agricultural season or growing period',
            
            # Measurement fields
            r'^value$': 'Numeric measurement value for the indicator',
            r'_value$': '{entity} measurement value',
            r'^price$': 'Price or cost in local currency',
            r'_price$': '{entity} price or cost',
            r'^quantity$': 'Quantity or amount measured',
            r'^amount$': 'Numeric amount or quantity',
            r'^count$': 'Count or number of items',
            r'_count$': 'Number of {entity}',
            
            # Units
            r'^unit$': 'Unit of measurement for the value field',
            r'_unit$': 'Unit of measurement for {entity}',
            
            # Status and Classification
            r'^status$': 'Current status or state of the record',
            r'^type$': 'Type or category classification',
            r'_type$': 'Type or classification of {entity}',
            r'^category$': 'Category or classification group',
            r'^phase$': 'Phase or stage classification',
            
            # Metadata
            r'^source$': 'Data source or origin',
            r'_source$': 'Source of {entity} data',
            r'^created': 'Timestamp when the record was created',
            r'^modified': 'Timestamp when the record was last modified',
            r'^updated': 'Timestamp of last update',
            r'ingested_at$': 'Timestamp when data was ingested into the warehouse',
            r'fetched_at$': 'Timestamp when data was fetched from the source',
        }
        
        # FAO-specific domain knowledge
        self.fao_domains = {
            'QC': 'Crops and Livestock Products',
            'QCL': 'Crops and Livestock Products (detailed)',
            'RL': 'Land Use',
            'RFN': 'Fertilizers and Nutrients',
            'RP': 'Pesticides',
            'TI': 'Trade Indices',
            'TCL': 'Trade - Crops and Livestock',
            'QI': 'Investment',
            'QV': 'Value of Agricultural Production',
            'RHN': 'Nutrition',
            'FBS': 'Food Balance Sheets'
        }
        
        # FEWS NET specific terms
        self.fews_terms = {
            'fnid': 'FEWS NET unique identifier',
            'ipc': 'Integrated Food Security Phase Classification',
            'cpcv2': 'Central Product Classification version 2 code',
            'dataseries': 'Data series identifier',
            'datasourcedocument': 'Source document identifier',
            'datasourceorganization': 'Source organization identifier',
            'geographic_unit': 'Geographic unit identifier',
            'fewsnet_region': 'FEWS NET regional classification',
        }
        
    def clean_field_name(self, field_name):
        """Clean and normalize field name for pattern matching."""
        if pd.isna(field_name):
            return ''
        return str(field_name).lower().strip()
    
    def extract_entity_from_field(self, field_name):
        """Extract entity name from field name (e.g., 'country_code' -> 'country')."""
        field_name = self.clean_field_name(field_name)
        
        # Remove common suffixes
        for suffix in ['_code', '_name', '_id', '_key', '_type', '_date', '_value', '_count', '_unit']:
            if field_name.endswith(suffix):
                entity = field_name[:-len(suffix)]
                return entity.replace('_', ' ')
        
        return field_name.replace('_', ' ')
    
    def generate_description(self, table_name, field_name, data_type):
        """Generate description based on table name, field name, and data type."""
        field_lower = self.clean_field_name(field_name)
        
        # Check FEWS NET specific terms first
        if 'fews' in table_name.lower():
            for term, desc in self.fews_terms.items():
                if term in field_lower:
                    return desc
        
        # Check FAO specific fields
        if 'fao_' in table_name.lower():
            if field_lower == 'domain_code':
                return 'FAO domain code identifier (e.g., QC, RL, RP) indicating the statistical domain category'
            elif field_lower == 'domain':
                return 'FAO domain name describing the statistical category (e.g., Crops and Livestock, Land Use, Pesticides)'
            elif field_lower == 'area_code':
                return 'FAO area code for the country or region'
            elif field_lower == 'area':
                return 'Country or geographic area name'
            elif field_lower == 'element_code':
                return 'FAO element code indicating the type of measurement (e.g., production, area harvested, yield)'
            elif field_lower == 'element':
                return 'Type of measurement or indicator (e.g., Area harvested, Production, Yield)'
            elif field_lower == 'item_code':
                return 'FAO item code for the commodity or product'
            elif field_lower == 'item':
                return 'Commodity or product name (e.g., Wheat, Maize, Rice)'
            elif field_lower == 'data_source':
                return 'Source of the FAO data (e.g., official statistics, FAO estimates)'
        
        # Try pattern matching
        for pattern, template in self.common_patterns.items():
            if re.match(pattern, field_lower):
                entity = self.extract_entity_from_field(field_name)
                description = template.replace('{entity}', entity)
                description = description.replace('{level}', field_lower.split('_')[-1] if '_' in field_lower else '')
                return description
        
        # Generate generic description based on field name
        entity = field_lower.replace('_', ' ')
        
        # Add context based on data type
        if data_type in ['INT64', 'INTEGER', 'FLOAT', 'FLOAT64', 'NUMERIC']:
            return f'Numeric value for {entity}'
        elif data_type in ['STRING', 'TEXT']:
            return f'Text field containing {entity} information'
        elif data_type in ['DATE', 'DATETIME', 'TIMESTAMP']:
            return f'Date/time value for {entity}'
        elif data_type == 'BOOLEAN':
            return f'Boolean flag indicating {entity} status'
        else:
            return f'Data field for {entity}'
    
    def classify_placement(self, field_name, data_type, description):
        """Classify field as Dimension or Fact based on patterns."""
        field_lower = self.clean_field_name(field_name)
        
        # Fact indicators (measures that change over time)
        fact_patterns = [
            r'^value$', r'_value$', r'^price$', r'_price$',
            r'^quantity$', r'^amount$', r'^production$', r'^yield$',
            r'^temperature$', r'^rainfall$', r'^precipitation$',
            r'^population$', r'^index$', r'^rate$', r'_rate$',
            r'pct_', r'percentage', r'^count$'
        ]
        
        for pattern in fact_patterns:
            if re.search(pattern, field_lower):
                return 'Fact'
        
        # Dimension indicators (descriptive, categorical, or slowly changing)
        dimension_patterns = [
            r'_id$', r'_code$', r'_name$', r'_key$',
            r'^country', r'^region', r'^admin_', r'^geographic',
            r'^year$', r'^month$', r'^day$', r'^date$', r'_date$',
            r'^season', r'^phase', r'^type$', r'_type$',
            r'^category', r'^status$', r'^source', r'^domain',
            r'^element', r'^item', r'^unit$', r'_unit$',
            r'^scenario', r'^classification'
        ]
        
        for pattern in dimension_patterns:
            if re.search(pattern, field_lower):
                return 'Dimension'
        
        # Default: if numeric and not clearly a dimension, likely a fact
        if data_type in ['INT64', 'INTEGER', 'FLOAT', 'FLOAT64', 'NUMERIC']:
            return 'Fact'
        
        return 'Dimension'
    
    def identify_unit(self, field_name, description):
        """Identify unit of measurement for quantitative fields."""
        field_lower = self.clean_field_name(field_name)
        desc_lower = description.lower() if description else ''
        
        # Common units based on field names
        unit_patterns = {
            r'rainfall|precipitation': 'mm (millimeters)',
            r'temperature': '°C (degrees Celsius)',
            r'area|land': 'hectares',
            r'production|quantity': 'tonnes',
            r'yield': 'tonnes/hectare',
            r'price': 'local currency',
            r'population|count': 'number of people',
            r'percentage|pct_': 'percentage (%)',
            r'index': 'index value',
            r'latitude|longitude': 'decimal degrees',
        }
        
        for pattern, unit in unit_patterns.items():
            if re.search(pattern, field_lower) or re.search(pattern, desc_lower):
                return unit
        
        return None
    
    def process_dataframe(self, df):
        """Process the entire dataframe and fill in missing descriptions."""
        print(f"Processing {len(df)} rows...")
        
        updates = 0
        for idx, row in df.iterrows():
            # Skip if description already exists
            if pd.notna(row['description']) and str(row['description']).strip() != '':
                continue
            
            # Generate description
            description = self.generate_description(
                row['table_name'],
                row['entity name'],
                row['data_type']
            )
            
            # Classify placement if missing
            placement = row['placemant'] if pd.notna(row['placemant']) else self.classify_placement(
                row['entity name'],
                row['data_type'],
                description
            )
            
            # Identify unit if missing
            unit = row['Unit'] if pd.notna(row['Unit']) else self.identify_unit(
                row['entity name'],
                description
            )
            
            # Update the dataframe
            df.at[idx, 'description'] = description
            if pd.isna(row['placemant']) or str(row['placemant']).strip() == '':
                df.at[idx, 'placemant'] = placement
            if pd.isna(row['Unit']) or str(row['Unit']).strip() == '':
                df.at[idx, 'Unit'] = unit if unit else ''
            
            updates += 1
            
            if updates % 100 == 0:
                print(f"  Processed {updates} rows...")
        
        print(f"✓ Generated descriptions for {updates} fields")
        return df


def main():
    """Main execution function."""
    print("=" * 80)
    print("FIELD DOCUMENTATION GENERATOR")
    print("=" * 80)
    
    # Read the Excel file
    input_file = 'data-eng/data/agricultural_indicator_schema_catalog.xlsx'
    output_file = 'data-eng/data/agricultural_indicator_schema_catalog_updated.xlsx'
    
    print(f"\nReading: {input_file}")
    df = pd.read_excel(input_file, sheet_name='Entity_mapping')
    
    print(f"Total rows: {len(df)}")
    missing_before = df['description'].isna().sum() + (df['description'] == '').sum()
    print(f"Missing descriptions before: {missing_before}")
    
    # Generate descriptions
    print("\nGenerating descriptions...")
    generator = FieldDescriptionGenerator()
    df_updated = generator.process_dataframe(df)
    
    # Statistics
    missing_after = df_updated['description'].isna().sum() + (df_updated['description'] == '').sum()
    print(f"\nMissing descriptions after: {missing_after}")
    print(f"Improvement: {missing_before - missing_after} descriptions added")
    
    # Save updated file
    print(f"\nSaving updated file: {output_file}")
    
    # Read all sheets from original file
    with pd.ExcelFile(input_file) as xls:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Write all original sheets
            for sheet_name in xls.sheet_names:
                sheet_name_str = str(sheet_name)  # Ensure it's a string
                if sheet_name_str == 'Entity_mapping':
                    df_updated.to_excel(writer, sheet_name=sheet_name_str, index=False)
                else:
                    pd.read_excel(xls, sheet_name=sheet_name_str).to_excel(
                        writer, sheet_name=sheet_name_str, index=False
                    )
    
    print(f"✓ File saved successfully!")
    
    # Summary by table
    print("\n" + "=" * 80)
    print("SUMMARY BY PRIORITY TABLES")
    print("=" * 80)
    
    priority_patterns = ['fao_', 'FEWS_NET']
    priority_df = df_updated[df_updated['table_name'].str.contains('|'.join(priority_patterns), case=False, na=False)]
    
    for table in sorted(priority_df['table_name'].unique()):
        table_df = priority_df[priority_df['table_name'] == table]
        missing = table_df['description'].isna().sum() + (table_df['description'] == '').sum()
        print(f"  {table}: {len(table_df)} fields, {missing} still missing")
    
    print("\n" + "=" * 80)
    print("DONE! Review the updated file and refine descriptions as needed.")
    print("=" * 80)


if __name__ == '__main__':
    main()
