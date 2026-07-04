# Data Warehouse Field Documentation Guide

## Overview

This guide explains how the automated field documentation system works and how to use it to maintain comprehensive documentation for all data warehouse fields.

## Generated Documentation

The field documentation generator has created descriptions for **9,180+ fields** across all tables in the data warehouse, with a focus on:
- FAO (Food and Agriculture Organization) tables
- FEWS NET (Famine Early Warning Systems Network) tables
- ILRI (International Livestock Research Institute) tables
- Other agricultural and food security data sources

## Files

### Documentation Files
- **Original**: `data-eng/data/agricultural_indicator_schema_catalog.xlsx`
- **Updated**: `data-eng/data/agricultural_indicator_schema_catalog_updated.xlsx`

### Scripts
- **Generator**: `data-eng/scripts/generate_field_documentation.py` - Main documentation generator
- **Analyzer**: `data-eng/scripts/analyze_documentation.py` - Analyze documentation completeness
- **Inspector**: `data-eng/scripts/inspect_excel.py` - Inspect Excel file structure

## How the Generator Works

### 1. Pattern-Based Description Generation

The generator uses intelligent pattern matching to create descriptions:

#### Common Patterns
- **IDs**: `id`, `*_id` → "Unique identifier for {entity}"
- **Codes**: `*_code` → "Standardized code for {entity}"
- **Names**: `*_name` → "Human-readable name for {entity}"
- **Geographic**: `country`, `region`, `admin_*` → Geographic location descriptions
- **Temporal**: `year`, `month`, `date`, `*_date` → Time-related descriptions
- **Measurements**: `value`, `price`, `quantity`, `amount` → Measurement descriptions

#### FAO-Specific Knowledge
- `domain_code` → "FAO domain code identifier (e.g., QC, RL, RP)"
- `domain` → "FAO domain name (e.g., Crops and Livestock, Land Use)"
- `area_code` → "FAO area code for the country or region"
- `element_code` → "FAO element code (e.g., production, area harvested, yield)"
- `item_code` → "FAO item code for the commodity or product"

#### FEWS NET-Specific Knowledge
- `fnid` → "FEWS NET unique identifier"
- `ipc` → "Integrated Food Security Phase Classification"
- `cpcv2` → "Central Product Classification version 2 code"
- `dataseries` → "Data series identifier"
- `fewsnet_region` → "FEWS NET regional classification"

### 2. Dimension vs Fact Classification

Fields are automatically classified based on their characteristics:

#### Dimension Fields (Descriptive, Categorical)
- IDs, codes, names, keys
- Geographic references (country, region, admin levels)
- Temporal references (year, month, date)
- Categories, types, statuses
- Classifications, phases, scenarios

#### Fact Fields (Measurements, Metrics)
- Values, prices, quantities, amounts
- Production, yield, temperature, rainfall
- Population, counts, rates, percentages
- Indexes, measurements

### 3. Unit Identification

Units are automatically identified for quantitative fields:

| Field Pattern | Unit |
|---------------|------|
| rainfall, precipitation | mm (millimeters) |
| temperature | °C (degrees Celsius) |
| area, land | hectares |
| production, quantity | tonnes |
| yield | tonnes/hectare |
| price | local currency |
| population, count | number of people |
| percentage, pct_ | percentage (%) |
| latitude, longitude | decimal degrees |

## Usage

### Running the Generator

```bash
# Generate documentation for all fields
python data-eng/scripts/generate_field_documentation.py
```

This will:
1. Read the original Excel file
2. Generate descriptions for all missing fields
3. Classify fields as Dimension or Fact
4. Identify units for quantitative fields
5. Save the updated file as `agricultural_indicator_schema_catalog_updated.xlsx`

### Analyzing Documentation

```bash
# Check documentation completeness
python data-eng/scripts/analyze_documentation.py
```

This shows:
- Total fields and missing descriptions
- Breakdown by priority tables (FAO, FEWS NET)
- Sample fields needing descriptions

### Inspecting the Excel File

```bash
# View Excel file structure
python data-eng/scripts/inspect_excel.py
```

## Customizing Descriptions

### Adding New Patterns

Edit `generate_field_documentation.py` and add patterns to `self.common_patterns`:

```python
self.common_patterns = {
    # Add your custom pattern
    r'^your_pattern$': 'Your description template for {entity}',
    # ... existing patterns
}
```

### Adding Domain-Specific Knowledge

For specific data sources, add to the appropriate dictionary:

```python
# FAO domains
self.fao_domains = {
    'NEW_CODE': 'New Domain Description',
    # ... existing domains
}

# FEWS NET terms
self.fews_terms = {
    'new_term': 'New term description',
    # ... existing terms
}
```

### Manual Refinement

After generation, you can manually refine descriptions in the Excel file:

1. Open `agricultural_indicator_schema_catalog_updated.xlsx`
2. Navigate to the `Entity_mapping` sheet
3. Edit descriptions in the `description` column
4. Save the file

## Best Practices

### Writing Good Descriptions

1. **Be Specific**: "ISO country code (alpha-2 or alpha-3 format)" vs "Country code"
2. **Add Context**: "FAO domain code identifier (e.g., QC, RL, RP)" vs "Domain code"
3. **Include Examples**: "Commodity name (e.g., Wheat, Maize, Rice)" vs "Commodity"
4. **Explain Purpose**: "Unit of measurement for the value field" vs "Unit"

### Dimension vs Fact Guidelines

**Dimension** (Use for):
- Identifiers and codes
- Names and labels
- Geographic and temporal references
- Categories and classifications
- Metadata fields

**Fact** (Use for):
- Numeric measurements
- Quantities and amounts
- Prices and costs
- Rates and percentages
- Calculated metrics

### Unit Guidelines

- Always specify units for quantitative fields
- Use standard units (hectares, tonnes, mm, °C)
- Include unit abbreviations in parentheses
- For currency, specify "local currency" if variable
- For percentages, use "percentage (%)"

## Maintenance

### Adding New Tables

When new tables are added to the warehouse:

1. Update the Excel file with new table/field entries
2. Run the generator script
3. Review and refine generated descriptions
4. Update this guide if new patterns are needed

### Updating Existing Descriptions

To update descriptions for existing fields:

1. Edit the generator script patterns if needed
2. Delete descriptions in the Excel file that need regeneration
3. Run the generator script
4. Review the updated descriptions

## FAO Domain Reference

| Code | Domain Name |
|------|-------------|
| QC | Crops and Livestock Products |
| QCL | Crops and Livestock Products (detailed) |
| RL | Land Use |
| RFN | Fertilizers and Nutrients |
| RP | Pesticides |
| TI | Trade Indices |
| TCL | Trade - Crops and Livestock |
| QI | Investment |
| QV | Value of Agricultural Production |
| RHN | Nutrition |
| FBS | Food Balance Sheets |

## Support

For questions or issues:
1. Review this guide
2. Check the generator script comments
3. Analyze the Excel file structure
4. Contact the data engineering team

## Version History

- **v1.0** (2026-07-04): Initial automated documentation generation
  - Generated 9,180 field descriptions
  - Implemented pattern-based generation
  - Added FAO and FEWS NET domain knowledge
  - Automated dimension/fact classification
  - Automated unit identification
