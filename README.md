# OpenTrace — Monorepo

This repository is intentionally split into **two sub-repos**:

- **Data Engineering**: [`data-eng/`](data-eng/README.md)
- **ML / AI Engineering**: [`ml-eng/`](ml-eng/README.md)

## Where to start

- For ingestion/orchestration/warehouse: start at **[`data-eng/README.md`](data-eng/README.md)**.
- For feature store/training/RAG/serving: start at **[`ml-eng/README.md`](ml-eng/README.md)**.

## Architecture (monorepo, simplest implementation view)

```mermaid
flowchart TD
  subgraph Monorepo[OpenTrace_Monorepo]
    subgraph DataEng[data-eng]
      Airbyte[Airbyte_Config]
      Airflow[Airflow_DAGs]
      dbt[dbt_Models]
      BQLanding[BQ_Landing]
      BQDev[BQ_Dev_raw_dev_staging_dev_mart_dev]
      BQProd[BQ_Prod_raw_prod_staging_prod_mart_prod]
      Terraform[Terraform_Infra]
    end

    subgraph MLEng[ml-eng]
      Features[Feature_Store_Code]
      Training[Training]
      Serving[Serving]
      Research&otherPapers[Research_Reports_pdfs]
      WebNewsMining[Web_News_Mining]
      TextProcessing[Text_Data_Processing]
      RAG[RAG]
      Qdrant[Qdrant_Cloud]
    end
  end

  Airbyte --> BQLanding
  Airflow --> Airbyte
  Airflow --> dbt
  dbt --> BQDev
  dbt --> BQProd
  BQLanding --> dbt

  BQDev --> Features
  BQProd --> Features
  Features --> Training
  Training --> Serving

  BQDev --> RAG
  BQProd --> RAG
  WebNewsMining --> TextProcessing
  Research&otherPapers --> TextProcessing
  TextProcessing --> Qdrant
  RAG --> Qdrant
  Qdrant --> RAG
```
