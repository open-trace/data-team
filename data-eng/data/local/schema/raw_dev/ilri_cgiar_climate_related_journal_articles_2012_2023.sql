CREATE SCHEMA IF NOT EXISTS "raw_dev";
CREATE TABLE IF NOT EXISTS "raw_dev"."ilri_cgiar_climate_related_journal_articles_2012_2023" (
  "Title" TEXT,
  "Authors" TEXT,
  "Author affiliations" TEXT,
  "Abstract" TEXT,
  "Funders" TEXT,
  "DOI" TEXT,
  "Year" BIGINT,
  "Journal" TEXT,
  "ISSN" TEXT,
  "Volume" TEXT,
  "Issue" TEXT,
  "Pages" TEXT,
  "Publisher" TEXT,
  "Keywords" TEXT,
  "Continents" TEXT,
  "Regions" TEXT,
  "Countries" TEXT,
  "Access rights" TEXT,
  "Usage rights" TEXT,
  "Repository link" TEXT,
  "Source" TEXT
);
