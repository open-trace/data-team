CREATE SCHEMA IF NOT EXISTS raw_dev;
CREATE TABLE IF NOT EXISTS raw_dev.cifor_icraf_raw (
  PLOT TEXT,
  Treatment TEXT,
  Soiltype TEXT,
  "%C" DOUBLE PRECISION,
  "%N" DOUBLE PRECISION,
  "%P" DOUBLE PRECISION,
  "%K" DOUBLE PRECISION,
  "%Ca" DOUBLE PRECISION,
  "%Mg" DOUBLE PRECISION,
  _source_doi TEXT,
  _source_file TEXT,
  _ingestion_time TIMESTAMP
);
