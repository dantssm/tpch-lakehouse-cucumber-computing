# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 Data profiling
# MAGIC
# MAGIC Goal: look at the bronze data before designing silver and gold, and record every decision we make because of it.
# MAGIC Each section is: question, query, observation, decision.

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../src"))

from tpch.config import CATALOG
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("USE SCHEMA bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Order dates and the as-of date
# MAGIC **Question:** what date range do the orders cover, and what is the last order date?
# MAGIC We want a fixed as-of date instead of `current_date()`, because the data is historical.

# COMMAND ----------

display(spark.sql("""
SELECT MIN(o_orderdate) AS first_order, MAX(o_orderdate) AS last_order,
       COUNT(*) AS orders, COUNT(DISTINCT o_custkey) AS customers_with_orders,
       (SELECT COUNT(*) FROM customer) AS customers_total
FROM orders
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Observation:** Orders run from 1992-01-01 to 1998-08-02 (7,500,000 orders). 499,989 of the 750,000 customers have at least one order, so about a third of customers have never ordered.
# MAGIC
# MAGIC **Decision:** The as-of date is 1998-08-02 (the last order date). An active customer is one with at least one order in the 12 months up to the as-of date. Customers without orders must not be lost in joins, so gold counts active customers from the orders side and keeps every customer in the customer dimension.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Complete periods
# MAGIC **Question:** are the first and last weeks and quarters complete? Our rule: compare complete periods only.
# MAGIC `days_with_orders` shows how many distinct days in each period have orders (a full week has 7, a full quarter 90-92).

# COMMAND ----------

weeks = "SELECT date_trunc('week', o_orderdate) AS week_start, COUNT(DISTINCT o_orderdate) AS days_with_orders FROM orders GROUP BY 1"
display(spark.sql(f"{weeks} ORDER BY 1 LIMIT 3"))   # first weeks
display(spark.sql(f"{weeks} ORDER BY 1 DESC LIMIT 3"))  # last weeks

# COMMAND ----------

display(spark.sql("""
SELECT date_trunc('quarter', o_orderdate) AS quarter_start, COUNT(DISTINCT o_orderdate) AS days_with_orders
FROM orders GROUP BY 1 ORDER BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Observation:** The first week (starting 1991-12-30) has orders on only 5 of 7 days, so it is partial. The last week (starting 1998-07-27) has all 7 days and ends on the as-of date. Every quarter has its full number of days except 1998-Q3, which has only 33 days (1 July to 2 August).
# MAGIC
# MAGIC **Decision:** Week-over-week comparisons use complete weeks only. The first week is excluded, and the latest complete week is 1998-07-27 to 1998-08-02. 1998-Q3 is never compared with a full quarter. Earlier quarter comparisons are valid, for example 1996-Q1 vs 1995-Q4 (91 and 92 days, both complete).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Categorical values
# MAGIC **Question:** which values do the categorical columns actually contain? These become the allowed lists in validation.

# COMMAND ----------

for tbl, col in [("customer", "c_mktsegment"), ("orders", "o_orderstatus"), ("orders", "o_orderpriority"),
                 ("lineitem", "l_returnflag"), ("lineitem", "l_linestatus"), ("lineitem", "l_shipmode")]:
    display(spark.sql(f"SELECT '{col}' AS column, {col} AS value, COUNT(*) AS n FROM {tbl} GROUP BY 2 ORDER BY 3 DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Observation:** Each categorical column contains only clean, uppercase values, and the counts add up to the table size, so there are no nulls or spelling variants.
# MAGIC - `c_mktsegment`: AUTOMOBILE, BUILDING, FURNITURE, HOUSEHOLD, MACHINERY
# MAGIC - `o_orderstatus`: F, O, P
# MAGIC - `o_orderpriority`: 1-URGENT, 2-HIGH, 3-MEDIUM, 4-NOT SPECIFIED, 5-LOW
# MAGIC - `l_returnflag`: A, N, R
# MAGIC - `l_linestatus`: F, O
# MAGIC - `l_shipmode`: AIR, FOB, MAIL, RAIL, REG AIR, SHIP, TRUCK
# MAGIC
# MAGIC **Decision:** The allowed lists are exactly the values we observed by grouping each column in bronze. Anything else fails validation (see `ALLOWED` in `validation.py`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Numeric ranges and nulls
# MAGIC **Question:** what are the min and max of discount, tax, quantity and prices, and are there nulls?
# MAGIC These give the bounds for the validation rules.

# COMMAND ----------

display(spark.sql("""
SELECT MIN(l_discount) AS min_discount, MAX(l_discount) AS max_discount,
       MIN(l_tax) AS min_tax, MAX(l_tax) AS max_tax,
       MIN(l_quantity) AS min_qty, MAX(l_quantity) AS max_qty,
       MIN(l_extendedprice) AS min_price, MAX(l_extendedprice) AS max_price,
       SUM(CASE WHEN l_discount IS NULL OR l_tax IS NULL OR l_quantity IS NULL OR l_extendedprice IS NULL THEN 1 ELSE 0 END) AS rows_with_nulls
FROM lineitem
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Observation:** discount 0.00 to 0.10, tax 0.00 to 0.08, quantity 1 to 50, extended price 900.99 to 104,949.50, and no nulls in any of them.
# MAGIC
# MAGIC **Decision:** Discount must be between 0.00 and 0.10 and tax between 0.00 and 0.08, which is the observed range and also matches the TPC-H benchmark specification. Quantity and extended price must be strictly positive. The bounds come from profiling the source, so they act as guardrails: a future load with a discount of 0.15 would fail validation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Referential integrity and basic checks
# MAGIC The rules in `validation.py`, run against bronze. Any failure here is a finding about the source data, not a bug in our code.

# COMMAND ----------

from tpch import validation
display(validation.run(spark))

# COMMAND ----------

# MAGIC %md
# MAGIC **Observation:** All 21 rules pass on bronze with 0 violations. Row counts match the source for all 8 tables, every order has a customer, every line item has an order, every order has at least one line item, key fields have no nulls, all categorical values are in the allowed lists, and discount, tax, quantity and price are within bounds. TPC-H sample data is clean, so no violations were expected.