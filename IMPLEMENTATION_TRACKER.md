# Implementation Tracker: Data Pipeline Expansion & RAG Namespace Separation

**Project Start Date:** July 1, 2026  
**Status:** 🟡 In Progress  
**Last Updated:** July 2, 2026 4:10 PM EAT  
**Approach:** Python Scripts + Cron Jobs (Direct BigQuery Ingestion)

---

## Executive Summary

### Project Goals
1. **Double data intake pipeline** across all priority source categories for Africa
2. **Audit all current data sources** - identify ingestion gaps by geography and crop type
3. **Implement namespace separation** in the RAG vector database for efficient filtering

### Key Decisions
- ✅ **Geography Focus:** Africa (all 54 countries)
- ✅ **Crop Coverage:** Comprehensive (all crop types)
- ✅ **Namespace Format:** Hierarchical (`<source_category>:<country_code>:<data_type>`)
- ✅ **Migration Strategy:** Apply to new ingestion only (no backfill of existing data)
- ✅ **Ingestion Method:** Python scripts with direct BigQuery writes
- ✅ **Scheduling:** Cron jobs (no Airflow, no Airbyte, no GitHub Actions)

### Timeline
- **Phase 1:** Data Source Audit & Gap Analysis - Week 1
- **Phase 2:** Python Ingestion Scripts + Cron Jobs - Weeks 2-3
- **Phase 3:** RAG Namespace Separation - Week 4
- **Testing & Validation:** Week 5
- **Deployment:** Week 6

---

## Phase 1: Data Source Audit & Gap Analysis

### Status: 🔵 Not Started

### Tasks

#### 1.1 BigQuery Coverage Analysis
- [ ] Create `data-eng/scripts/audit_bq_coverage.py`
  - Query all tables in `landing` dataset
  - Extract country codes from geographic fields
  - Extract crop types from relevant fields
  - Generate coverage matrix by source category
- [ ] **Files Created:** None yet
- [ ] **Testing:** Manual execution against BigQuery
- [ ] **Notes:**

#### 1.2 Gap Report Generator
- [ ] Create `data-eng/scripts/generate_gap_report.py`
  - Load priority sources for 54 African countries
  - Cross-reference with current coverage
  - Identify missing countries per category
  - Identify missing crop types
  - Output JSON and CSV reports
- [ ] **Files Created:** None yet
- [ ] **Testing:** Validate against known gaps
- [ ] **Notes:**

#### 1.3 Priority Sources Configuration
- [ ] Create `data-eng/config/priority_sources_africa.yaml`
  - Define 54 African countries (ISO codes)
  - Define priority crop types
  - Define source categories (market, weather, satellite, food_security, soil, crop)
  - Map existing sources to categories
- [ ] **Files Created:** None yet
- [ ] **Testing:** YAML validation
- [ ] **Notes:**

#### 1.4 Coverage Analysis Notebook
- [ ] Create `data-eng/notebooks/africa_coverage_analysis.ipynb`
  - Visual gap analysis (heatmaps, charts)
  - Country coverage by category
  - Crop coverage by country
  - Priority recommendations
- [ ] **Files Created:** None yet
- [ ] **Testing:** Execute notebook end-to-end
- [ ] **Notes:**

### Deliverables
- [ ] Coverage audit script
- [ ] Gap analysis report (JSON/CSV)
- [ ] Priority sources configuration
- [ ] Visual analysis notebook

### Issues & Resolutions
_None yet_

---

## Phase 2: Python Ingestion Scripts + Cron Jobs

### Status: 🔵 Not Started

### Tasks

#### 2.1 Python Ingestion Scripts
- [ ] Create `data-eng/data-pipelines/ingestion/fews_net_ingestion.py`
  - Accept country codes as CLI parameters
  - Fetch FEWS NET data via API
  - Transform to BigQuery schema
  - Load to `landing` dataset
  - Log results and errors
- [ ] Create `data-eng/data-pipelines/ingestion/wfp_food_prices_ingestion.py`
  - Expand WFP market price coverage
  - Support multiple African countries
  - Direct BigQuery writes
- [ ] Create `data-eng/data-pipelines/ingestion/isric_soil_ingestion.py`
  - Expand ISRIC soil data coverage
  - Geographic expansion across Africa
- [ ] Create `data-eng/data-pipelines/ingestion/gbif_biodiversity_ingestion.py`
  - Expand GBIF species occurrence data
  - Country-specific queries
- [ ] Create `data-eng/data-pipelines/ingestion/fao_crop_ingestion.py`
  - Expand FAO crop production data
  - Multi-country support
- [ ] Create `data-eng/data-pipelines/ingestion/common/bigquery_client.py`
  - Shared BigQuery utilities
  - Connection management
  - Error handling
- [ ] Create `data-eng/data-pipelines/ingestion/common/api_helpers.py`
  - Common API patterns
  - Rate limiting
  - Retry logic
- [ ] Create `data-eng/data-pipelines/ingestion/common/country_config.py`
  - Africa country mappings (ISO codes)
  - Country-specific API endpoints
- [ ] **Files Created:** None yet
- [ ] **Testing:** Unit tests for each script
- [ ] **Notes:**

#### 2.2 Cron Job Configuration
- [ ] Create `data-eng/config/crontab.txt`
  - Daily: FEWS NET ingestion (6:00 AM)
  - Daily: WFP food prices (7:00 AM)
  - Weekly: ISRIC soil data (Sunday 2:00 AM)
  - Weekly: GBIF biodiversity (Sunday 3:00 AM)
  - Monthly: FAO crop data (1st of month, 1:00 AM)
- [ ] Create `data-eng/scripts/setup_cron_jobs.sh`
  - Install cron jobs from crontab.txt
  - Set up logging directories
  - Configure environment variables
  - Validate cron syntax
- [ ] Create `data-eng/scripts/run_ingestion_wrapper.sh`
  - Wrapper script for cron execution
  - Activate virtual environment
  - Set PYTHONPATH
  - Capture logs
  - Send error notifications
- [ ] **Files Created:** None yet
- [ ] **Testing:** Cron syntax validation
- [ ] **Notes:**

#### 2.3 Configuration Files
- [ ] Create `data-eng/config/africa_countries.yaml`
  - All 54 African countries
  - ISO 3166-1 alpha-2 codes
  - ISO 3166-1 alpha-3 codes
  - Country names (English)
- [ ] Create `data-eng/config/ingestion_sources.yaml`
  - Source definitions
  - API endpoints
  - Authentication methods
  - Rate limits
- [ ] Create `data-eng/config/country_source_matrix.yaml`
  - Map which sources cover which countries
  - Priority rankings
  - Data availability status
- [ ] **Files Created:** None yet
- [ ] **Testing:** YAML validation
- [ ] **Notes:**

#### 2.4 Logging & Monitoring
- [ ] Create `data-eng/scripts/check_ingestion_logs.py`
  - Parse cron job logs
  - Identify failures
  - Generate daily summary
- [ ] Create `data-eng/config/logging_config.yaml`
  - Log file locations
  - Rotation policies
  - Log levels per source
- [ ] **Files Created:** None yet
- [ ] **Testing:** Log parsing validation
- [ ] **Notes:**

#### 2.5 Documentation
- [ ] Create `data-eng/docs/PYTHON_INGESTION_SETUP.md`
  - Python script usage
  - Cron job setup guide
  - Environment variables
  - Troubleshooting
- [ ] Create `data-eng/data-pipelines/ingestion/README.md`
  - Script documentation
  - API requirements
  - Testing instructions
- [ ] **Files Created:** None yet
- [ ] **Testing:** Documentation review
- [ ] **Notes:**

### Deliverables
- [ ] Python ingestion scripts (5 sources + 3 common modules)
- [ ] Cron job configuration (crontab.txt + setup scripts)
- [ ] Configuration files (3 YAML files)
- [ ] Logging & monitoring scripts
- [ ] Documentation (2 markdown files)

### Issues & Resolutions
_None yet_

---

## Phase 3: RAG Namespace Separation

### Status: 🔵 Not Started

### Tasks

#### 3.1 Namespace Mapper
- [ ] Create `ml-eng/ml/rag/ingestion/namespace_mapper.py`
  - Implement hierarchical namespace logic
  - Country code extraction/normalization
  - Source category detection
  - Data type classification
  - Validation functions
- [ ] **Files Created:** None yet
- [ ] **Testing:** Unit tests for namespace generation
- [ ] **Notes:**

#### 3.2 Qdrant Collection Schema Updates
- [ ] Update `ml-eng/ml/rag/scripts/qdrant_collection_specs.py`
  - Add `source_category` payload index (keyword)
  - Add `country_code` payload index (keyword)
  - Add `data_type` payload index (keyword)
  - Keep `namespace` for backward compatibility
  - Update all collection builders (news, research, OTA, bq_table_descriptions)
- [ ] **Files Modified:** None yet
- [ ] **Testing:** Collection creation test
- [ ] **Notes:**

#### 3.3 Vector Database Loader Updates
- [ ] Update `ml-eng/ml/rag/text_processors/news_load_to_vector_db.py`
  - Import namespace_mapper
  - Populate source_category, country_code, data_type fields
  - Populate hierarchical namespace field
- [ ] Update `ml-eng/ml/rag/text_processors/research_papers_load_to_vector_db.py`
  - Same namespace field population
- [ ] Update `ml-eng/ml/rag/text_processors/data_descriptions_load_to_vector_db.py`
  - Same namespace field population
- [ ] Update `ml-eng/ml/rag/text_processors/ota_insights_load_to_vector_db.py`
  - Same namespace field population
- [ ] **Files Modified:** None yet
- [ ] **Testing:** Load test data with new fields
- [ ] **Notes:**

#### 3.4 Retrieval Logic Updates
- [ ] Update `ml-eng/ml/rag/retrievers/vector_retriever.py`
  - Add filter support for source_category
  - Add filter support for country_code
  - Add filter support for data_type
  - Maintain backward compatibility with namespace
  - Update query methods
- [ ] **Files Modified:** None yet
- [ ] **Testing:** Retrieval tests with filters
- [ ] **Notes:**

#### 3.5 RAG Graph Updates
- [ ] Update `ml-eng/ml/rag/chatbot/graph.py`
  - Use new namespace filters in retrieval nodes
  - Update _retrieve_vector_cascade calls
  - Update _vector_retrieve_for_corpus calls
  - Test with decomposed queries
- [ ] **Files Modified:** None yet
- [ ] **Testing:** End-to-end RAG pipeline test
- [ ] **Notes:**

### Deliverables
- [ ] Namespace mapper module
- [ ] Updated Qdrant collection specs
- [ ] Updated vector database loaders (4 files)
- [ ] Updated retrieval logic
- [ ] Updated RAG graph

### Issues & Resolutions
_None yet_

---

## Testing & Validation

### Status: 🔵 Not Started

### Unit Tests
- [ ] Test namespace_mapper functions
  - Country code extraction
  - Source category detection
  - Hierarchical namespace generation
- [ ] Test vector retriever filters
  - source_category filtering
  - country_code filtering
  - data_type filtering
  - Combined filters
- [ ] Test Python ingestion scripts
  - API connection tests
  - Data transformation tests
  - BigQuery write tests

### Integration Tests
- [ ] Test full ingestion pipeline
  - Run Python scripts manually
  - Verify BigQuery data loaded
  - Check data quality
- [ ] Test cron job execution
  - Simulate cron environment
  - Verify logging
  - Check error handling
- [ ] Test RAG pipeline end-to-end
  - Query with geographic filters
  - Query with source category filters
  - Verify correct namespace filtering

### Manual Verification
- [ ] Inspect Qdrant collections
  - Verify payload indexes created
  - Verify namespace fields populated
  - Check data distribution
- [ ] Test retrieval queries
  - Filter by country (e.g., Kenya, Nigeria)
  - Filter by category (e.g., market, weather)
  - Combined filters
- [ ] Monitor cron job execution
  - Check cron logs
  - Verify scheduled runs
  - Test failure notifications

### Performance Testing
- [ ] Measure query latency with filters
- [ ] Verify index performance
- [ ] Check memory usage
- [ ] Monitor BigQuery costs

---

## Deployment Checklist

### Pre-Deployment
- [ ] Code review completed
- [ ] All tests passing
- [ ] Documentation updated
- [ ] Backup current Qdrant collections
- [ ] Backup current BigQuery data

### Deployment Sequence
1. [ ] Deploy Python ingestion scripts to server/VM
2. [ ] Set up virtual environment and dependencies
3. [ ] Configure environment variables (BigQuery credentials, API keys)
4. [ ] Install cron jobs using setup script
5. [ ] Deploy namespace mapper module
6. [ ] Update Qdrant collection specs (create new indexes)
7. [ ] Deploy updated vector database loaders
8. [ ] Deploy updated retrieval logic
9. [ ] Deploy updated RAG graph

### Post-Deployment Validation
- [ ] Verify cron jobs are running
- [ ] Check ingestion logs for errors
- [ ] Verify new data has namespace fields
- [ ] Test retrieval with namespace filters
- [ ] Monitor BigQuery data flow
- [ ] Check error logs

### Rollback Plan
- [ ] Stop cron jobs (crontab -r)
- [ ] Revert code changes (git)
- [ ] Restore Qdrant collections from backup
- [ ] Restore BigQuery data if needed
- [ ] Restart services

---

## Issues & Resolutions Log

### Issue #1
**Date:**  
**Description:**  
**Resolution:**  
**Status:**

---

## Next Steps & Future Work

### Immediate Next Steps
1. Create BigQuery coverage audit script
2. Generate gap analysis report
3. Build Python ingestion scripts
4. Set up cron jobs
5. Build namespace mapper
6. Update Qdrant collection specs

### Future Enhancements
- [ ] Automated gap monitoring dashboard
- [ ] Real-time ingestion metrics
- [ ] Advanced namespace hierarchies (e.g., sub-regions)
- [ ] Crop-specific vector collections
- [ ] Multi-language support for African languages
- [ ] Email/Slack notifications for cron failures
- [ ] Web dashboard for monitoring ingestion status

### Technical Debt
- [ ] Backfill existing data with namespaces (if needed later)
- [ ] Optimize Qdrant index configurations
- [ ] Consolidate duplicate source definitions
- [ ] Add retry logic for failed ingestions
- [ ] Implement incremental ingestion (avoid full reloads)

---

## Progress Summary

**Overall Progress:** 0/60+ tasks completed (0%)

### Phase Breakdown
- **Phase 1:** 0/4 deliverables ⚪
- **Phase 2:** 0/5 deliverables ⚪
- **Phase 3:** 0/5 deliverables ⚪
- **Testing:** 0/4 categories ⚪
- **Deployment:** 0/3 stages ⚪

---

## Notes & Observations

### July 2, 2026 - Initial Setup
- Confirmed no existing cron jobs or Python ingestion scripts in codebase
- `data-eng/data-pipelines/ingestion/` directory is empty (only .gitkeep)
- Current setup uses Airflow (which we're replacing with cron jobs)
- All ingestion infrastructure needs to be built from scratch

---

**Legend:**
- 🔵 Not Started
- 🟡 In Progress
- 🟢 Completed
- 🔴 Blocked
- ⚪ Pending
