---
status: accepted
---

# Answers read the mart

Ask ADZA's fact evidence is read from the mart (`mart_dev` / `BQ_DATASET_GOLD`). Raw (`raw_dev`, also called bronze) is the immutable landed source and is not a query target for answers.

An earlier architecture note treated bronze as the ML contract and told retrieval to query `raw_dev`. The bind contracts, tests, and live path already use the mart. Consumers read the refined dataset; raw exists so a bad transform can be replayed. Pointing answers back at raw would mean rebuilding every bind. Gold is another name for the mart, not a second dataset and not a layer answers must avoid.
