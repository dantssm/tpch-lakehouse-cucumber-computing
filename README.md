# tpch-lakehouse-cucumber-computing

Bronze / silver / gold lakehouse pipeline on Databricks for the TPC-H dataset (`samples.tpch`).
Group Assignment 1 by **Cucumber Computing**. This repo covers the **CTO / Strategist** gold layer and business questions.

> Presentation: [`docs/presentation.pdf`](docs/presentation.pdf)

## Architecture

```
samples.tpch (read-only source)
      │  copy as is + _ingested_at
      ▼
  bronze.*   raw copy, no changes
      │  clean, type, enforce constraints (3NF)
      ▼
  silver.*   3NF, enforced data quality
      │  aggregate for CTO questions
      ▼
   gold.*    fact + dimensions + headline metrics
```

## Repository structure

```
src/tpch/        pipeline code (config, bronze, silver, gold, validation, alerts)
sql/             gold, validation and alert queries
notebooks/       pipeline runner, exploration/profiling, business questions, alert demo
docs/            presentation, ER diagram of silver tables
```

## Setup

Requirements: [uv](https://docs.astral.sh/uv/), Python 3.12, a Databricks workspace.

```bash
git clone <repo-url>
cd tpch-lakehouse-cucumber-computing
uv sync
```

### Databricks

1. Workspace → Create → **Git folder** → paste the repo URL.
2. Confirm the target catalog exists (Free Edition default: `workspace`).
3. Open `notebooks/00_run_pipeline` and Run all (attach **Serverless**).

## Running the pipeline

From `notebooks/00_run_pipeline`: runs bronze (silver and gold once built) and displays the validation results.

Equivalent local calls (require a Spark session, e.g. via Databricks Connect):
```bash
uv run python -m tpch.bronze
```