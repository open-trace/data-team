CREATE SCHEMA IF NOT EXISTS "landing";
CREATE TABLE IF NOT EXISTS "landing"."farmlover_www_fao_org_giews_data_tools_en_document_text" (
  "page_number" TEXT,
  "line_number" TEXT,
  "text" TEXT,
  "_fl_source_id" TEXT,
  "_fl_source_url" TEXT,
  "_fl_input_file" TEXT,
  "_fl_input_sha256" TEXT,
  "_fl_source_report" TEXT,
  "_fl_collection_run_id" TEXT,
  "_fl_retrieved_at_utc" TIMESTAMPTZ,
  "_fl_input_row_number" BIGINT,
  "_fl_table_label" TEXT,
  "_fl_data_kind" TEXT,
  "_fl_review_status" TEXT,
  "_fl_snapshot_id" TEXT,
  "_fl_prepared_at_utc" TIMESTAMPTZ
);
