# Table Relationships Documentation

## Overview

This document provides detailed documentation of all discovered relationships between tables in the agricultural data warehouse. These relationships were discovered through automated analysis of field names and patterns across 157 tables.

---

## Summary Statistics

- **Total Relationships Discovered**: 21,852
- **High Confidence**: 10,662 (49%)
- **Medium Confidence**: 11,080 (51%)
- **Low Confidence**: 110 (0.5%)

### Relationship Discovery Methods

1. **Exact Match** (10,662 relationships)
   - Field names match exactly between tables
   - Example: `country_code` in Table A → `country_code` in Table B
   - Highest confidence level

2. **Pattern Match** (11,048 relationships)
   - Field names match after normalization (removing _code, _name, _id suffixes)
   - Example: `country_code` → `country`, `item_code` → `item`
   - Medium confidence level

3. **Inferred** (142 relationships)
   - Based on domain knowledge and common patterns
   - Example: FAO tables sharing common structure
   - Lower confidence, requires validation

---

## Common Relationship Patterns

### 1. Geographic Relationships

#### Country-Level Relationships
**Primary Field**: `country_code`
- **Format**: ISO 3166-1 alpha-2 or alpha-3 codes
- **Tables Using**: 99+ tables
- **Relationship Type**: Many-to-One (many records → one country)

**Example Relationships**:
```
fao_fertilizers_nutrient_bronze.country_code → fao_land_use_bronze.country_code
FEWS_NET_Market_Prices.country_code → fao_trade_crops_livestock_bronze.country_code
wfp_food_prices.country → FEWS_NET_Food_insecure_population_estimates.country
```

#### Area Code Relationships
**Primary Field**: `area_code`
- **Format**: FAO area codes
- **Tables Using**: FAO and Climate tables
- **Relationship Type**: Many-to-One

**Example Relationships**:
```
fao_fertilizers_nutrient_bronze.area_code → fao_land_use_bronze.area_code
Climate_emissions_table.area_code → fao_pesticides_use_bronze.area_code
```

#### Administrative Level Relationships (FEWS NET)
**Primary Fields**: `admin_0`, `admin_1`, `admin_2`, `admin_3`
- **Format**: Administrative hierarchy names
- **Tables Using**: FEWS NET tables
- **Relationship Type**: Hierarchical (admin_0 → admin_1 → admin_2 → admin_3)

**Example Relationships**:
```
FEWS_NET_Food_insecure_population.admin_0 → FEWS_NET_Market_Prices.admin_0
FEWS_NET_IPC_Classification.admin_1 → FEWS_NET_Geographic_Units.admin_1
```

#### FEWS NET ID (FNID)
**Primary Field**: `fnid`
- **Format**: Unique FEWS NET geographic identifier
- **Tables Using**: FEWS NET tables
- **Relationship Type**: One-to-Many

---

### 2. Temporal Relationships

#### Year-Based Relationships
**Primary Field**: `year`
- **Format**: 4-digit year (e.g., 2023)
- **Tables Using**: FAO, Climate, and time-series tables
- **Relationship Type**: Many-to-Many (multiple tables share same years)

**Example Relationships**:
```
fao_fertilizers_nutrient_bronze.year → fao_land_use_bronze.year
Climate_emissions.year → fao_trade_indices_bronze.year
```

#### Date-Based Relationships
**Primary Field**: `date`, `observation_date`, `report_date`
- **Format**: DATE or TIMESTAMP
- **Tables Using**: Market prices, surveys, observations
- **Relationship Type**: Many-to-Many

---

### 3. Product/Item Relationships

#### Item Code (FAO)
**Primary Field**: `item_code`
- **Format**: FAO commodity/product codes
- **Tables Using**: FAO tables
- **Relationship Type**: Many-to-One

**Example Relationships**:
```
fao_trade_crops_livestock_bronze.item_code → fao_fertilizers_nutrient_bronze.item_code
fao_pesticides_use_bronze.item_code → fao_land_use_bronze.item_code
```

#### Product/Commodity Names
**Primary Fields**: `product`, `commodity`, `item`
- **Format**: Text names
- **Tables Using**: Market price tables, trade tables
- **Relationship Type**: Many-to-One

---

### 4. Classification Relationships

#### Element Code (FAO)
**Primary Field**: `element_code`
- **Format**: FAO element classification codes
- **Tables Using**: FAO tables
- **Relationship Type**: Many-to-One
- **Examples**: Production, Import, Export, Yield, etc.

#### Domain Code (FAO)
**Primary Field**: `domain_code`
- **Format**: FAO statistical domain codes
- **Tables Using**: FAO tables
- **Relationship Type**: Many-to-One
- **Examples**: QC (Crops and Livestock), RL (Land Use), etc.

#### IPC Phase (FEWS NET)
**Primary Field**: `ipc_phase`
- **Format**: Integer 1-5 (food insecurity phases)
- **Tables Using**: FEWS NET food security tables
- **Relationship Type**: Many-to-One

---

## Relationship Cardinality

### One-to-One (1:1)
**Rare in this dataset**
- Typically found in lookup/reference tables
- Example: Country code → Country name

### One-to-Many (1:N)
**Most Common**
- Dimension table → Fact table relationships
- Examples:
  - One country → Many fertilizer records
  - One year → Many trade records
  - One product → Many price observations

### Many-to-Many (N:M)
**Common in time-series data**
- Multiple tables sharing common dimensions
- Examples:
  - FAO tables sharing country_code and year
  - FEWS NET tables sharing fnid and date
  - Climate tables sharing area_code and element_code

---

## High-Confidence Relationships

### Top 10 Most Connected Tables

1. **fao_fertilizers_nutrient_bronze** (796 connections)
   - Connects to: All FAO tables, Climate tables
   - Key fields: country_code, area_code, item_code, element_code, year

2. **fao_land_use_bronze** (796 connections)
   - Connects to: All FAO tables, Climate tables
   - Key fields: country_code, area_code, item_code, element_code, year

3. **fao_pesticides_use_bronze** (796 connections)
   - Connects to: All FAO tables, Climate tables
   - Key fields: country_code, area_code, item_code, element_code, year

4. **fao_trade_crops_livestock_bronze** (796 connections)
   - Connects to: All FAO tables, Climate tables
   - Key fields: country_code, area_code, item_code, element_code, year

5. **fao_trade_indices_bronze** (796 connections)
   - Connects to: All FAO tables, Climate tables
   - Key fields: country_code, area_code, item_code, element_code, year

6-10. **Climate Emissions Tables** (731 connections each)
   - Farm gate emissions from crops
   - Farm gate emissions from livestock
   - Land use and change emissions
   - Emissions from fires and forests

---

## Relationship Validation

### Validation Status

- **Validated**: Relationships confirmed through BigQuery data analysis
- **Discovered**: Relationships identified through field name analysis
- **Inferred**: Relationships based on domain knowledge

### Validation Process

1. **Field Name Analysis** ✅ Complete
   - Analyzed 9,312 fields across 157 tables
   - Identified 21,852 potential relationships

2. **BigQuery Validation** ⏳ Optional
   - Script available: `query_bq_relationships.py`
   - Validates referential integrity
   - Determines actual cardinality
   - Requires BigQuery credentials

3. **Manual Review** 📝 Recommended
   - Review high-confidence relationships
   - Validate business logic
   - Confirm relationship semantics

---

## Using Relationships in Queries

### Example 1: Joining FAO Tables

```sql
-- Join fertilizer and land use data by country and year
SELECT 
    f.country_code,
    f.year,
    f.item as fertilizer_type,
    f.value as fertilizer_value,
    l.item as land_use_type,
    l.value as land_use_value
FROM fao_fertilizers_nutrient_bronze f
INNER JOIN fao_land_use_bronze l
    ON f.country_code = l.country_code
    AND f.year = l.year
WHERE f.year >= 2020
```

### Example 2: Joining FEWS NET Tables

```sql
-- Join food insecurity and market prices by country and date
SELECT 
    fi.country,
    fi.admin_1,
    fi.ipc_phase,
    fi.population,
    mp.commodity,
    mp.price,
    mp.date
FROM FEWS_NET_Food_insecure_population_estimates fi
LEFT JOIN FEWS_NET_Market_Prices mp
    ON fi.country_code = mp.country_code
    AND fi.fnid = mp.fnid
WHERE fi.ipc_phase >= 3  -- Crisis or worse
```

### Example 3: Cross-Source Join

```sql
-- Join FAO trade data with FEWS NET food security
SELECT 
    t.country_code,
    t.year,
    t.item as traded_commodity,
    t.value as trade_value,
    f.ipc_phase,
    f.population as food_insecure_population
FROM fao_trade_crops_livestock_bronze t
LEFT JOIN FEWS_NET_Food_insecure_population_estimates f
    ON t.country_code = f.country_code
    AND CAST(t.year AS STRING) = EXTRACT(YEAR FROM f.date)
WHERE t.year >= 2020
```

---

## Relationship Quality Indicators

### High Quality Indicators
✅ Exact field name matches  
✅ Consistent data types  
✅ High referential integrity (>80% match rate)  
✅ Well-documented in source systems  
✅ Frequently used in existing queries  

### Medium Quality Indicators
⚠️ Pattern-based matches (normalized field names)  
⚠️ Some data type mismatches  
⚠️ Moderate referential integrity (50-80% match rate)  
⚠️ Limited documentation  

### Low Quality Indicators
❌ Inferred relationships  
❌ Inconsistent data types  
❌ Low referential integrity (<50% match rate)  
❌ No documentation  
❌ Requires validation  

---

## Maintenance and Updates

### When to Update Relationships

1. **New Tables Added**
   - Run `analyze_table_relationships.py`
   - Review discovered relationships
   - Update documentation

2. **Schema Changes**
   - Field names changed
   - New fields added
   - Data types modified

3. **Data Quality Issues**
   - Referential integrity problems
   - Unexpected null values
   - Cardinality changes

### Update Process

```bash
# Step 1: Analyze relationships
python data-eng/scripts/analyze_table_relationships.py

# Step 2: (Optional) Validate with BigQuery
python data-eng/scripts/query_bq_relationships.py

# Step 3: Regenerate ERD
python data-eng/scripts/generate_erd_code.py

# Step 4: Update documentation
# Edit this file and TABLE_CATEGORIZATION.md
```

---

## Troubleshooting Common Issues

### Issue 1: Relationship Not Found
**Symptom**: Expected relationship missing from discovered relationships  
**Solution**: 
- Check field name spelling
- Verify data types match
- Run relationship analysis again
- Manually add to refined relationships CSV

### Issue 2: Too Many Relationships
**Symptom**: Table has hundreds of relationships  
**Solution**:
- Filter by confidence level (use only high confidence)
- Focus on direct relationships
- Review for false positives

### Issue 3: Join Performance Issues
**Symptom**: Queries using relationships are slow  
**Solution**:
- Add indexes on join fields
- Partition tables by common join keys
- Use clustering on frequently joined fields
- Consider materialized views

---

## Related Files

- **Discovered Relationships**: `table_relationships_discovered.csv` (21,852 relationships)
- **Validated Relationships**: `table_relationships_validated.csv` (if BigQuery validation run)
- **ERD Diagram**: `ERD_diagram.dbml` (visual representation)
- **Analysis Script**: `analyze_table_relationships.py`
- **Validation Script**: `query_bq_relationships.py`

---

**Last Updated**: July 4, 2026  
**Maintained By**: Data Engineering Team  
**Version**: 1.0
