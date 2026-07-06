#!/usr/bin/env python3
"""Analyze the current state of documentation in the Excel file."""

import pandas as pd

# Read the Entity_mapping sheet
df = pd.read_excel('data-eng/data/agricultural_indicator_schema_catalog.xlsx', sheet_name='Entity_mapping')

print("=" * 80)
print("DOCUMENTATION ANALYSIS")
print("=" * 80)

# Overall statistics
missing = df['description'].isna() | (df['description'] == '')
print(f"\nTotal rows: {len(df)}")
print(f"Missing descriptions: {missing.sum()}")
print(f"Percentage missing: {missing.sum()/len(df)*100:.1f}%")
print(f"Completed descriptions: {(~missing).sum()}")

# Priority tables analysis
print("\n" + "=" * 80)
print("PRIORITY TABLES (FAO & FEWS NET)")
print("=" * 80)

priority_patterns = ['fao_', 'FEWS_NET']
priority_df = df[df['table_name'].str.contains('|'.join(priority_patterns), case=False, na=False)]
priority_missing = priority_df['description'].isna() | (priority_df['description'] == '')

print(f"\nPriority tables rows: {len(priority_df)}")
print(f"Missing descriptions: {priority_missing.sum()}")
print(f"Percentage missing: {priority_missing.sum()/len(priority_df)*100:.1f}%")

print("\nBreakdown by table:")
for table in sorted(priority_df['table_name'].unique()):
    table_df = priority_df[priority_df['table_name'] == table]
    table_missing = table_df['description'].isna() | (table_df['description'] == '')
    print(f"  {table}: {len(table_df)} fields, {table_missing.sum()} missing ({table_missing.sum()/len(table_df)*100:.0f}%)")

# Sample of fields needing descriptions
print("\n" + "=" * 80)
print("SAMPLE FIELDS NEEDING DESCRIPTIONS (FAO)")
print("=" * 80)

fao_df = df[df['table_name'].str.startswith('fao_', na=False)]
fao_missing = fao_df[fao_df['description'].isna() | (fao_df['description'] == '')]
print(fao_missing[['table_name', 'entity name', 'data_type', 'placemant']].head(20).to_string(index=False))

print("\n" + "=" * 80)
print("SAMPLE FIELDS NEEDING DESCRIPTIONS (FEWS NET)")
print("=" * 80)

fews_df = df[df['table_name'].str.contains('FEWS_NET', na=False)]
fews_missing = fews_df[fews_df['description'].isna() | (fews_df['description'] == '')]
print(fews_missing[['table_name', 'entity name', 'data_type', 'placemant']].head(20).to_string(index=False))
