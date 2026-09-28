# tpch-lakehouse-cucumber-computing

Bronze / silver / gold lakehouse pipeline on Databricks for the TPC-H dataset (`samples.tpch`).
Group Assignment 1 by **Cucumber Computing**. This repo covers the **CTO / Strategist** gold layer and business questions.

> Presentation: [`docs/presentation.pdf`](docs/presentation.pdf)

## Repository structure

```
src/tpch/        pipeline code (config, bronze, silver, gold, validation, alerts)
sql/             gold, validation and alert queries
notebooks/       pipeline runner, business questions, alert demo
docs/            presentation, ER diagram of silver tables
```