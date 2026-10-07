# tpch-lakehouse-cucumber-computing

Bronze / silver / gold lakehouse pipeline on Databricks for the TPC-H dataset (`samples.tpch`).
Group Assignment 1 by **Cucumber Computing**. Business questions are answered from the gold layer for the **CTO / Strategist** role.

Presentation and team responsibilities: [`docs/presentation.md`](docs/presentation.md)

## Architecture

```
samples.tpch (read-only source)
      │  copy as is + _ingested_at
      ▼
  bronze.*   raw copy, no changes
      │  clean, type, enforce constraints (3NF), bad rows to quarantine
      ▼
  silver.*   3NF, enforced data quality
      │  aggregate for CTO questions
      ▼
   gold.*    metric tables for the CTO questions
```

| Gold table | Content |
|---|---|
| `revenue_metrics` | Net revenue per day, with its week and quarter |
| `customer_activity` | Last order date and active flag per customer (customers without orders are kept) |
| `top_suppliers` / `top_parts` | Revenue contribution per supplier and per part |

## Repository structure

```
src/tpch/        pipeline code (config, bronze, silver, gold, validation, alerts, orchestrator)
notebooks/       00_run_pipeline (runner), 01_data_profiling, 02_business_questions, silver_info
docs/            presentation link and team responsibilities
```

## Setup

Requirements: [uv](https://docs.astral.sh/uv/), Python 3.12, a Databricks workspace.

```bash
git clone https://github.com/dantssm/tpch-lakehouse-cucumber-computing.git
cd tpch-lakehouse-cucumber-computing
uv sync
```

### Databricks

1. Workspace → Create → **Git folder** → paste the repo URL.
2. Confirm the target catalog exists (Free Edition default: `workspace`).
3. Open `notebooks/00_run_pipeline` and run the last cell (attach **Serverless**).

Catalog, source and as-of date are set in `src/tpch/config.py`. Only `samples.tpch` is read, so the same code runs in another workspace with a different catalog name.

## Running the pipeline

From `notebooks/00_run_pipeline`: the last cell runs bronze, silver, gold, validation and the revenue alert in order.

```python
from tpch import orchestrator
orchestrator.run_full_pipeline(spark)                      # everything
orchestrator.run_full_pipeline(spark, run_bronze=False)    # skip bronze (reuse the existing copy)
```

Notes for the team workspace:
- The schemas `bronze`, `silver`, `gold` are created once by the workspace owner (`config.ensure_schemas`). Regular runs need no CREATE SCHEMA right.
- A table is owned by the person who created it, and only the owner can replace it. Each layer is therefore rebuilt by one person (bronze: workspace owner), the others run with `run_bronze=False` or only read.

Equivalent local calls (require a Spark session, e.g. via Databricks Connect):
```bash
uv run python -m tpch.bronze
```

## Definitions (single source of truth)

Derived by profiling bronze (see `notebooks/01_data_profiling`). Every query in this repo uses these.

| Term | Definition |
|---|---|
| **Revenue** | `SUM(l_extendedprice * (1 - l_discount))`, i.e. net of discount, before tax |
| **As-of date** | `1998-08-02` (last order date in the data), set in `config.py`. We never use `current_date()` |
| **Active customer** | Customer with at least one order in the trailing 12 months up to the as-of date: latest order date **strictly after** `as_of - 365 days` (`> 1997-08-02`) and not after the as-of date. A customer whose last order is exactly on 1997-08-02 is not counted (301 such customers) |
| **Complete period** | A week or quarter with orders on every calendar day it covers. The first week (from 1991-12-30, 5 days) and 1998-Q3 (33 days) are partial. Partial periods are excluded from period-over-period comparisons |
| **Revenue alert** | Week-over-week net revenue change below -20%, computed over complete weeks only (`src/tpch/alerts.py`) |

## Validation

Rules live in `src/tpch/validation.py` and run from `notebooks/00_run_pipeline` (and as step 5 of the orchestrator). Each rule counts violating rows; 0 = pass. **52 rules**:

| Layer | Rules | What is checked |
|---|---|---|
| Bronze | 21 | Row count equals the source (8 tables); every order has a valid customer, every line item a valid order, every order at least one line item; keys, customer and date not null; quantity and price strictly positive; discount 0.00 to 0.10 and tax 0.00 to 0.08; 6 categorical columns contain only their allowed values |
| Bronze to silver | 8 | Per table: bronze rows = silver rows + quarantine rows, so no row is lost silently |
| Silver | 17 | Primary key unique on all 8 tables; nation to region, customer to nation, supplier to nation and lineitem to partsupp references exist; discount and tax in bounds; quantity positive; order price not negative; ship date not after receipt date and not before the order date |
| Gold | 6 | Gold revenue matches the silver sum within $0.01; revenue and contribution values are strictly positive; no NULL in the activity flag; revenue table not empty |

Silver also enforces NOT NULL, primary key, foreign key and CHECK constraints at table level. Rows that break a rule are written to `silver.quarantine_<table>` with a `rejection_reason`.

### How the bounds and allowed values were derived

Both come from profiling the bronze tables (`notebooks/01_data_profiling`), not from assumptions:

- **Numeric bounds:** `SELECT MIN(...), MAX(...)` on the lineitem columns gave discount 0.00 to 0.10, tax 0.00 to 0.08, quantity 1 to 50 and extended price 900.99 to 104,949.50, with no NULLs. The discount and tax ranges are set in `config.py` and also match the TPC-H specification. Quantity and price must be strictly positive. A future load with a discount of 0.15 would fail the rule.
- **Allowed values:** `SELECT DISTINCT` on each categorical column listed the values actually present (for example 7 ship modes, 5 market segments, 5 order priorities). These lists are in `ALLOWED` in `validation.py`.

## CTO / Strategist questions

Queries and answers are in `notebooks/02_business_questions`.

| # | Question | How it is answered |
|---|---|---|
| 1 | Revenue 1996-Q1 vs the previous quarter | Quarterly sums of `gold.revenue_metrics`, with the change in % |
| 2 | Active customers as of 1998-08-02 | Count of `gold.customer_activity` where `is_active_trailing_12m` |
| 3 | Alert for a 20% week-over-week drop | Same 20% rule as `src/tpch/alerts.py`, shown on data with one week artificially cut |
| 4 | Three company-health metrics | Net revenue trend, active customers (trailing 12 months), contribution of top suppliers and parts |

### Monitoring and alerting

Headline metrics are the gold tables above: revenue trend (`revenue_metrics`), active customers (`customer_activity`) and top contributors (`top_suppliers`, `top_parts`). The alert `alerts.run(spark)` flags any complete week whose net revenue fell more than 20% against the previous week, and runs as the last step of the orchestrator.