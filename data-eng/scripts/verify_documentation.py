#!/usr/bin/env python3
"""Verify the updated documentation file."""

import pandas as pd

# Read the updated file
df = pd.read_excel('data-eng/data/agricultural_indicator_schema_catalog_updated.xlsx', sheet_name='Entity_mapping')

print("=" * 80)
print("VERIFICATION REPORT")
print("=" * 80)

print(f"\nTotal rows: {len(df)}")
print(f"Rows with descriptions: {df['description'].notna().sum()}")
print(f"Missing descriptions: {df['description'].isna().sum()}")

print("\n" + "=" * 80)
print("SAMPLE OF FIRST 10 ROWS")
print("=" * 80)
print(df[['table_name', 'entity name', 'description']].head(10).to_string(index=False))

print("\n" + "=" * 80)
print("FILE LOCATION")
print("=" * 80)
print("Updated file: data-eng/data/agricultural_indicator_schema_catalog_updated.xlsx")
print("Full path: C:\\Users\\BEST\\OneDrive\\Desktop\\data-team\\data-eng\\data\\agricultural_indicator_schema_catalog_updated.xlsx")
