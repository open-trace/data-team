# Table Categorization Guide

## Overview

This document provides a comprehensive categorization of all 157 tables in the agricultural data warehouse, organized by data source, table type (Fact vs Dimension), and purpose.

---

## Summary Statistics

- **Total Tables**: 157
- **Total Fields**: 9,312
- **Total Relationships**: 21,852

### Tables by Source
- **Other**: 74 tables (47%)
- **ILRI**: 52 tables (33%)
- **Climate**: 12 tables (8%)
- **FEWS NET**: 8 tables (5%)
- **FAO**: 5 tables (3%)
- **ISRIC**: 4 tables (3%)
- **WFP**: 1 table (1%)
- **GBIF**: 1 table (1%)

### Tables by Type
- **Dimension Tables**: 124 (79%)
- **Fact Tables**: 33 (21%)

---

## Table Categories

### 1. FAO (Food and Agriculture Organization) Tables

**Purpose**: Agricultural statistics, trade data, land use, fertilizers, and pesticides

**Tables** (5):
1. `fao_fertilizers_nutrient_bronze` - **Fact Table**
   - Fertilizer nutrient data by country and time period
   - 796 relationships with other tables
   
2. `fao_land_use_bronze` - **Fact Table**
   - Land use statistics across countries
   - 796 relationships with other tables
   
3. `fao_pesticides_use_bronze` - **Fact Table**
   - Pesticide usage data
   - 796 relationships with other tables
   
4. `fao_trade_crops_livestock_bronze` - **Fact Table**
   - Trade data for crops and livestock
   - 796 relationships with other tables
   
5. `fao_trade_indices_bronze` - **Fact Table**
   - Trade indices and economic indicators
   - 796 relationships with other tables

**Common Fields**: `country_code`, `area_code`, `item_code`, `element_code`, `year`, `value`

**Relationships**: FAO tables are highly interconnected, sharing common dimension fields like country codes, item codes, and time periods.

---

### 2. FEWS NET (Famine Early Warning Systems Network) Tables

**Purpose**: Food security, population estimates, market prices, and IPC classifications

**Tables** (8):
1. `FEWS_NET_Food_insecure_population_estimates_data_series` - **Fact Table**
   - Food insecurity population estimates
   - Acute food insecurity phases across Africa
   
2. `FEWS_NET_IPC_Acute_Food_Insecurity_Classification` - **Dimension Table**
   - IPC phase classifications and definitions
   
3. `FEWS_NET_Market_Prices` - **Fact Table**
   - Market price data for food commodities
   
4. Additional FEWS NET tables for geographic units, scenarios, and time periods

**Common Fields**: `fnid` (FEWS NET ID), `country`, `country_code`, `admin_0`, `admin_1`, `admin_2`, `ipc_phase`, `population`

**Relationships**: FEWS NET tables relate through geographic identifiers (fnid, admin levels) and country codes.

---

### 3. ILRI (International Livestock Research Institute) Tables

**Purpose**: Livestock research, surveys, baseline data, and agricultural development

**Tables** (52):
- Largest category with diverse research datasets
- Includes survey data, baseline studies, and research findings
- Covers livestock, agriculture, and rural development topics

**Common Fields**: `country`, `region`, `survey_id`, `respondent_id`, `date`

**Relationships**: ILRI tables often relate through country and region fields, with some tables having specific research project identifiers.

---

### 4. Climate & Environmental Tables

**Purpose**: Climate change data, emissions, environmental indicators

**Tables** (12):
- Climate change agrifood systems emissions
- Farm gate emissions from crops and livestock
- Land use and change emissions
- Emissions from fires, forests, and drained organic soils

**Common Fields**: `country_code`, `area_code`, `year`, `element`, `value`, `unit`

**Relationships**: Climate tables share common structure with FAO tables, often relating through country codes and time periods.

---

### 5. ISRIC (International Soil Reference and Information Centre) Tables

**Purpose**: Soil data, soil properties, and land characteristics

**Tables** (4):
- Soil property data
- Soil classification information
- Geographic soil distribution

**Common Fields**: `country_code`, `soil_type`, `depth`, `property`, `value`

**Relationships**: ISRIC tables relate to other tables through country codes and geographic coordinates.

---

### 6. WFP (World Food Programme) Tables

**Purpose**: Food prices and market data

**Tables** (1):
- `wfp_food_prices` - Market price data for food commodities

**Common Fields**: `country`, `market`, `commodity`, `price`, `date`

**Relationships**: Relates to FEWS NET market data and country dimension tables.

---

### 7. GBIF (Global Biodiversity Information Facility) Tables

**Purpose**: Biodiversity and species occurrence data

**Tables** (1):
- Species occurrence data by country and region

**Common Fields**: `country_code`, `species`, `occurrence_count`, `date`

**Relationships**: Relates through country codes to other geographic tables.

---

### 8. Other Tables

**Purpose**: Miscellaneous data sources including development indicators, human development index, and various research datasets

**Tables** (74):
- Human development indicators
- Economic indicators
- Social indicators
- Research-specific datasets

**Common Fields**: Varies by table, but commonly includes `country`, `year`, `indicator`, `value`

---

## Fact vs Dimension Table Classification

### Fact Tables (33 tables)

**Characteristics**:
- Contain measurable, quantitative data (values, prices, quantities)
- Have foreign keys to dimension tables
- Typically large in row count
- Change frequently with new data
- Examples: `fao_fertilizers_nutrient_bronze`, `FEWS_NET_Market_Prices`, `wfp_food_prices`

**Common Patterns**:
- Fields ending in `_value`, `_amount`, `_price`, `_quantity`
- Numeric measurements with units
- Time-series data with date/year fields
- Multiple foreign keys to dimension tables

### Dimension Tables (124 tables)

**Characteristics**:
- Contain descriptive, categorical data
- Provide context for fact tables
- Relatively static (change infrequently)
- Smaller in row count
- Examples: Country lookups, IPC classifications, product catalogs

**Common Patterns**:
- Fields ending in `_code`, `_name`, `_id`, `_key`
- Descriptive text fields
- Classification and categorization data
- Lookup tables for codes and names

---

## Key Relationships

### Geographic Relationships
- **Country Code**: Primary linking field across most tables
- **Admin Levels**: FEWS NET admin_0, admin_1, admin_2 hierarchy
- **FNID**: FEWS NET geographic unit identifier

### Temporal Relationships
- **Year**: Common time dimension across FAO and climate tables
- **Date**: Used in market prices and survey data
- **Period**: Time periods in FEWS NET data

### Product/Item Relationships
- **Item Code**: FAO product/commodity codes
- **Product**: Market commodity names
- **Commodity**: Food items in price data

### Classification Relationships
- **Element Code**: FAO data element classifications
- **Domain Code**: FAO statistical domains
- **IPC Phase**: Food insecurity classifications

---

## Usage Guidelines

### For Data Analysts
1. **Start with dimension tables** to understand available categories and codes
2. **Join fact tables** using common keys (country_code, year, item_code)
3. **Check data availability** by source and time period
4. **Use the ERD diagram** to visualize relationships

### For Data Engineers
1. **Maintain referential integrity** when loading new data
2. **Update dimension tables first**, then fact tables
3. **Monitor relationship quality** using the discovered relationships CSV
4. **Document new tables** following the categorization patterns

### For Data Scientists
1. **Understand grain/granularity** of each fact table
2. **Aggregate appropriately** based on dimension hierarchies
3. **Consider data quality** and completeness by source
4. **Use relationships** to enrich analysis with contextual data

---

## Maintenance Notes

### Adding New Tables
1. Determine source category (FAO, FEWS NET, ILRI, etc.)
2. Classify as Fact or Dimension based on content
3. Identify key fields and relationships
4. Update this categorization document
5. Regenerate ERD if needed

### Updating Relationships
1. Run `analyze_table_relationships.py` to discover new relationships
2. Validate high-confidence relationships
3. Update ERD diagrams
4. Document any new relationship patterns

---

## Related Documentation

- **ERD Diagram**: `ERD_diagram.dbml` (visual representation)
- **Relationships**: `table_relationships_discovered.csv` (detailed relationship data)
- **Field Documentation**: `agricultural_indicator_schema_catalog_updated.xlsx` (field-level details)
- **ERD Summary**: `ERD_summary.md` (statistics and overview)

---

**Last Updated**: July 4, 2026  
**Maintained By**: Data Engineering Team  
**Version**: 1.0
