# ERD (Entity-Relationship Diagram) Usage Guide

## Overview

This guide explains how to use the Entity-Relationship Diagram (ERD) for the agricultural data warehouse. The ERD visualizes the relationships between 157 tables containing 9,312 fields and 21,852 relationships.

---

## Quick Start

### 1. View the ERD Online

**Using dbdiagram.io** (Recommended):
1. Open https://dbdiagram.io
2. Click "Import" or paste code directly
3. Copy the contents of `data-eng/docs/ERD_diagram.dbml`
4. Paste into dbdiagram.io
5. The diagram will render automatically

**Using Mermaid** (GitHub/Markdown):
1. Open `data-eng/docs/ERD_diagram_mermaid.md`
2. View in GitHub (renders automatically)
3. Or use any Mermaid-compatible viewer

### 2. Understanding the Diagram

The ERD shows:
- **Tables** (boxes): Represent database tables
- **Fields** (lines within boxes): Key fields only (for readability)
- **Relationships** (lines between boxes): How tables connect
- **Annotations**: Source category and table type (Fact/Dimension)

---

## Reading the ERD

### Table Representation

```
Table fao_fertilizers_nutrient_bronze {
  // Source: FAO
  // Type: Fact
  id integer [pk]
  country_code varchar
  area_code varchar
  item_code varchar
  element_code varchar
  year integer
  value float
  // ... and 15 more fields
}
```

**Components**:
- **Table Name**: `fao_fertilizers_nutrient_bronze`
- **Source**: FAO (Food and Agriculture Organization)
- **Type**: Fact (contains measurable data)
- **Primary Key**: `id` marked with `[pk]`
- **Key Fields**: Important fields for relationships
- **Field Count**: Total fields in the table

### Relationship Representation

```
Ref: fao_fertilizers_nutrient_bronze.country_code > fao_land_use_bronze.country_code
```

**Interpretation**:
- **Source Table**: `fao_fertilizers_nutrient_bronze`
- **Source Field**: `country_code`
- **Target Table**: `fao_land_use_bronze`
- **Target Field**: `country_code`
- **Relationship**: Many-to-One (many fertilizer records → one country)

### Relationship Symbols

- `>` : Many-to-One relationship
- `<` : One-to-Many relationship
- `-` : One-to-One relationship
- `<>` : Many-to-Many relationship

---

## Common Use Cases

### Use Case 1: Finding Related Tables

**Scenario**: You're working with `fao_fertilizers_nutrient_bronze` and want to know what other tables you can join with.

**Steps**:
1. Locate `fao_fertilizers_nutrient_bronze` in the ERD
2. Follow the relationship lines to connected tables
3. Note the connecting fields (e.g., `country_code`, `year`)
4. Check the relationship type (many-to-one, etc.)

**Result**: You'll find it connects to:
- `fao_land_use_bronze` (via country_code, year)
- `fao_pesticides_use_bronze` (via country_code, year)
- `fao_trade_crops_livestock_bronze` (via country_code, year)
- Climate tables (via area_code, year)

### Use Case 2: Understanding Data Flow

**Scenario**: You need to understand how food security data flows from FEWS NET to other systems.

**Steps**:
1. Find FEWS NET tables in the ERD (look for `FEWS_NET_` prefix)
2. Identify the fact tables (food insecurity estimates, market prices)
3. Trace relationships to dimension tables (geographic units, IPC classifications)
4. Follow connections to other data sources (FAO, WFP)

**Result**: You'll see:
- FEWS NET data connects via `country_code` and `fnid`
- Geographic hierarchy through `admin_0`, `admin_1`, `admin_2`
- Links to market price data and trade statistics

### Use Case 3: Planning a New Analysis

**Scenario**: You want to analyze the relationship between fertilizer use and crop yields across African countries.

**Steps**:
1. Identify relevant tables:
   - `fao_fertilizers_nutrient_bronze` (fertilizer data)
   - `fao_trade_crops_livestock_bronze` (crop production)
2. Find common fields in the ERD:
   - `country_code` (geographic dimension)
   - `year` (temporal dimension)
   - `item_code` (crop/product dimension)
3. Plan your join strategy
4. Check for additional context tables (country names, product descriptions)

**Result**: Clear understanding of how to join tables and what dimensions are available.

### Use Case 4: Data Quality Investigation

**Scenario**: You notice missing data in your analysis and want to understand why.

**Steps**:
1. Identify the tables involved in your analysis
2. Check the ERD for relationship types (one-to-many, many-to-many)
3. Look for optional vs required relationships
4. Verify if you're using the correct join fields

**Result**: Understanding of data completeness and appropriate join strategies (INNER vs LEFT JOIN).

---

## ERD Sections

The ERD is organized by data source for easier navigation:

### 1. FAO Section (Top)
- 5 highly connected fact tables
- Common fields: country_code, area_code, item_code, element_code, year
- 796 relationships each

### 2. FEWS NET Section (Middle-Left)
- 8 tables for food security and market data
- Common fields: fnid, country_code, admin levels, ipc_phase
- Geographic hierarchy structure

### 3. Climate Section (Middle-Right)
- 12 tables for emissions and environmental data
- Similar structure to FAO tables
- 731 relationships each

### 4. ILRI Section (Bottom-Left)
- 52 research and survey tables
- Diverse structure
- Country and region-based relationships

### 5. Other Sources (Bottom-Right)
- WFP, GBIF, ISRIC, and miscellaneous tables
- Various relationship patterns

---

## Tips for Using the ERD

### 1. Start with High-Level View
- Don't zoom into details immediately
- Understand the overall structure first
- Identify major table groups (FAO, FEWS NET, etc.)

### 2. Focus on Your Domain
- If working with food security, focus on FEWS NET section
- If working with agriculture statistics, focus on FAO section
- Use the source annotations to navigate

### 3. Trace Relationships Carefully
- Follow relationship lines from source to target
- Note the direction of relationships
- Check field names match your expectations

### 4. Use Complementary Documentation
- **ERD Summary** (`ERD_summary.md`): Statistics and overview
- **Table Categorization** (`TABLE_CATEGORIZATION.md`): Detailed table descriptions
- **Table Relationships** (`TABLE_RELATIONSHIPS.md`): Relationship details and examples
- **Field Documentation** (Excel file): Field-level descriptions

### 5. Validate with Data
- ERD shows potential relationships
- Always validate with actual data queries
- Check referential integrity in your specific use case

---

## Customizing the ERD

### Generating a Focused ERD

If the full ERD is too complex, generate a focused version:

```python
# Edit generate_erd_code.py
# Change max_tables parameter

# For FAO tables only
generator.generate_dbml('ERD_FAO_only.dbml', max_tables=10)

# For FEWS NET tables only
generator.generate_dbml('ERD_FEWSNET_only.dbml', max_tables=10)
```

### Exporting the ERD

**From dbdiagram.io**:
1. Open your diagram
2. Click "Export" in the top menu
3. Choose format:
   - PNG (for presentations)
   - PDF (for documentation)
   - SQL (for database creation)

**Recommended Settings**:
- **Size**: Large (for readability)
- **Theme**: Light (for printing)
- **Layout**: Auto-arrange first, then manually adjust

---

## Common Patterns in the ERD

### Pattern 1: Star Schema
**Description**: Fact table in center, dimension tables around it

**Example**:
```
dim_country ──┐
              ├──> fact_fertilizers
dim_year ─────┘
```

**Usage**: Typical data warehouse pattern for analytics

### Pattern 2: Snowflake Schema
**Description**: Dimension tables have their own dimensions

**Example**:
```
dim_country ──> dim_region ──> fact_data
```

**Usage**: Normalized dimension hierarchies

### Pattern 3: Shared Dimensions
**Description**: Multiple fact tables share the same dimensions

**Example**:
```
                ┌──> fact_fertilizers
dim_country ────┼──> fact_land_use
                └──> fact_pesticides
```

**Usage**: Common in our FAO and Climate tables

---

## Troubleshooting

### Issue 1: ERD Too Complex
**Solution**: 
- Use filtered views (FAO only, FEWS NET only)
- Focus on high-confidence relationships only
- Generate separate ERDs for each data source

### Issue 2: Can't Find a Table
**Solution**:
- Use Ctrl+F in dbdiagram.io
- Check table name spelling
- Verify table is in the top 50 most connected (ERD shows top 50 by default)

### Issue 3: Relationship Doesn't Make Sense
**Solution**:
- Check the confidence level in `table_relationships_discovered.csv`
- Validate with actual data
- Report issues for manual review

### Issue 4: ERD Won't Load
**Solution**:
- Check DBML syntax in the file
- Try smaller subset of tables
- Use Mermaid version instead

---

## Best Practices

### For Data Analysts
✅ Use ERD to plan joins before writing queries  
✅ Verify relationship directions (many-to-one vs one-to-many)  
✅ Check for missing relationships that might cause data loss  
✅ Document your join logic based on ERD  

### For Data Engineers
✅ Keep ERD updated when schema changes  
✅ Validate new relationships before adding to ERD  
✅ Use ERD to identify optimization opportunities  
✅ Share ERD with stakeholders for data literacy  

### For Data Scientists
✅ Understand data lineage through ERD  
✅ Identify feature engineering opportunities  
✅ Plan data aggregations based on relationships  
✅ Use ERD to explain model inputs  

---

## Updating the ERD

### When to Update
- New tables added to the warehouse
- Schema changes (fields added/removed/renamed)
- New relationships discovered
- Quarterly review (recommended)

### How to Update

```bash
# Step 1: Analyze relationships
python data-eng/scripts/analyze_table_relationships.py

# Step 2: Generate new ERD
python data-eng/scripts/generate_erd_code.py

# Step 3: Review changes
# Compare new ERD with previous version

# Step 4: Update documentation
# Update this guide if needed
```

---

## Additional Resources

### Files
- **DBML Code**: `data-eng/docs/ERD_diagram.dbml`
- **Mermaid Code**: `data-eng/docs/ERD_diagram_mermaid.md`
- **Summary**: `data-eng/docs/ERD_summary.md`
- **Relationships CSV**: `data-eng/docs/table_relationships_discovered.csv`

### Scripts
- **Relationship Analyzer**: `data-eng/scripts/analyze_table_relationships.py`
- **ERD Generator**: `data-eng/scripts/generate_erd_code.py`
- **BigQuery Validator**: `data-eng/scripts/query_bq_relationships.py`

### External Tools
- **dbdiagram.io**: https://dbdiagram.io (ERD visualization)
- **Mermaid Live Editor**: https://mermaid.live (Mermaid diagrams)
- **DBML Documentation**: https://dbml.dbdiagram.io/docs (DBML syntax)

---

## FAQ

**Q: Why are only 50 tables shown in the ERD?**  
A: For readability. The full dataset has 157 tables. We show the 50 most connected tables. You can generate custom ERDs with different table counts.

**Q: How accurate are the relationships?**  
A: Relationships are discovered through field name analysis. High-confidence relationships (exact matches) are very accurate. Medium-confidence relationships should be validated with data.

**Q: Can I edit the ERD?**  
A: Yes! In dbdiagram.io, you can manually adjust the layout, add/remove tables, and modify relationships. Export your customized version.

**Q: How do I share the ERD with my team?**  
A: Export as PNG/PDF from dbdiagram.io, or share the DBML file for them to import.

**Q: What if I find an incorrect relationship?**  
A: Document it and update the `table_relationships_discovered.csv` file. Regenerate the ERD to reflect changes.

---

**Last Updated**: July 4, 2026  
**Maintained By**: Data Engineering Team  
**Version**: 1.0
