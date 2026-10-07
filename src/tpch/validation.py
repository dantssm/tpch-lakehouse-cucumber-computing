# src/tpch/validation.py
"""Data validation. Each rule is a SQL query returning the number of violating rows: 0 = pass."""
from pyspark.sql import DataFrame, SparkSession, functions as F

from tpch.config import CATALOG, SOURCE, TABLES, fq, DISCOUNT_MIN, DISCOUNT_MAX, TAX_MIN, TAX_MAX


ALLOWED = {
    ("customer", "c_mktsegment"): ["BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD", "AUTOMOBILE"],
    ("orders", "o_orderstatus"): ["F", "O", "P"],
    ("orders", "o_orderpriority"): ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"],
    ("lineitem", "l_returnflag"): ["N", "R", "A"],
    ("lineitem", "l_linestatus"): ["F", "O"],
    ("lineitem", "l_shipmode"): ["TRUCK", "FOB", "REG AIR", "AIR", "SHIP", "RAIL", "MAIL"],
}


def _checks(spark: SparkSession) -> list[tuple[str, str, str]]:
    o, c, li = fq("bronze", "orders"), fq("bronze", "customer"), fq("bronze", "lineitem")
    
    checks = [
        ("bronze", f"{t}: row count equals source",
         f"SELECT ABS((SELECT COUNT(*) FROM {SOURCE}.{t}) - (SELECT COUNT(*) FROM {fq('bronze', t)}))")
        for t in TABLES
    ]
    
    # Safely get existing tables in silver schema to check for quarantine tables
    try:
        silver_tables = [row.tableName for row in spark.sql(f"SHOW TABLES IN {CATALOG}.silver").collect()]
    except Exception:
        silver_tables = []
    
    # Bronze rows = Silver + Quarantine reconciliation for each table
    for t in TABLES:
        b_tbl = fq("bronze", t)
        s_tbl = fq("silver", t)
        q_name = f"quarantine_{t}"
        
        q_sql = f"(SELECT COUNT(*) FROM {fq('silver', q_name)})" if q_name in silver_tables else "0"
        
        checks.append((
            "bronze_silver_reconciliation",
            f"{t}: bronze rows equals silver plus quarantine",
            f"""SELECT ABS(
                (SELECT COUNT(*) FROM {b_tbl}) - 
                (SELECT COUNT(*) FROM {s_tbl}) - 
                {q_sql}
            )"""
        ))

    checks += [
        ("bronze", "orders: customer exists",
         f"SELECT COUNT(*) FROM {o} LEFT ANTI JOIN {c} ON o_custkey = c_custkey"),
        ("bronze", "lineitem: order exists",
         f"SELECT COUNT(*) FROM {li} LEFT ANTI JOIN {o} ON l_orderkey = o_orderkey"),
        ("bronze", "orders: has at least one line item",
         f"SELECT COUNT(*) FROM {o} LEFT ANTI JOIN {li} ON o_orderkey = l_orderkey"),
        ("bronze", "orders: key, customer and date not null",
         f"SELECT COUNT(*) FROM {o} WHERE o_orderkey IS NULL OR o_custkey IS NULL OR o_orderdate IS NULL"),
        ("bronze", "lineitem: quantity and price strictly positive",
         f"SELECT COUNT(*) FROM {li} WHERE NOT coalesce(l_quantity > 0 AND l_extendedprice > 0, false)"),
        ("bronze", f"lineitem: discount between {DISCOUNT_MIN} and {DISCOUNT_MAX}",
         f"SELECT COUNT(*) FROM {li} WHERE NOT coalesce(l_discount BETWEEN {DISCOUNT_MIN} AND {DISCOUNT_MAX}, false)"),
        ("bronze", f"lineitem: tax between {TAX_MIN} and {TAX_MAX}",
         f"SELECT COUNT(*) FROM {li} WHERE NOT coalesce(l_tax BETWEEN {TAX_MIN} AND {TAX_MAX}, false)"),
    ]
    
    checks += [
        ("bronze", f"{t}.{col}: value in allowed list",
         f"SELECT COUNT(*) FROM {fq('bronze', t)} WHERE NOT coalesce({col} IN ({', '.join(map(repr, vals))}), false)")
        for (t, col), vals in ALLOWED.items()
    ]

    s_region = fq("silver", "region")
    s_nation = fq("silver", "nation")
    s_supplier = fq("silver", "supplier")
    s_customer = fq("silver", "customer")
    s_part = fq("silver", "part")
    s_partsupp = fq("silver", "partsupp")
    s_orders = fq("silver", "orders")
    s_lineitem = fq("silver", "lineitem")

    checks += [
        ("silver", "region: primary key unique", f"SELECT COUNT(*) - COUNT(DISTINCT r_regionkey) FROM {s_region}"),
        ("silver", "nation: primary key unique", f"SELECT COUNT(*) - COUNT(DISTINCT n_nationkey) FROM {s_nation}"),
        ("silver", "supplier: primary key unique", f"SELECT COUNT(*) - COUNT(DISTINCT s_suppkey) FROM {s_supplier}"),
        ("silver", "customer: primary key unique", f"SELECT COUNT(*) - COUNT(DISTINCT c_custkey) FROM {s_customer}"),
        ("silver", "part: primary key unique", f"SELECT COUNT(*) - COUNT(DISTINCT p_partkey) FROM {s_part}"),
        ("silver", "partsupp: primary key unique", f"SELECT COUNT(*) - COUNT(DISTINCT CONCAT(ps_partkey, '-', ps_suppkey)) FROM {s_partsupp}"),
        ("silver", "orders: primary key unique", f"SELECT COUNT(*) - COUNT(DISTINCT o_orderkey) FROM {s_orders}"),
        ("silver", "lineitem: primary key unique", f"SELECT COUNT(*) - COUNT(DISTINCT CONCAT(l_orderkey, '-', l_linenumber)) FROM {s_lineitem}"),
    ]

    checks += [
        ("silver", "nation: every nation references a valid region",
         f"SELECT COUNT(*) FROM {s_nation} n LEFT ANTI JOIN {s_region} r ON n.n_regionkey = r.r_regionkey"),
        ("silver", "lineitem: ship_date before or equal receipt_date",
         f"SELECT COUNT(*) FROM {s_lineitem} WHERE l_shipdate > l_receiptdate"),
        ("silver", f"lineitem: discount and tax within bounds [{DISCOUNT_MIN}, {DISCOUNT_MAX}] and [{TAX_MIN}, {TAX_MAX}]",
         f"SELECT COUNT(*) FROM {s_lineitem} WHERE NOT (l_discount BETWEEN {DISCOUNT_MIN} AND {DISCOUNT_MAX} AND l_tax BETWEEN {TAX_MIN} AND {TAX_MAX})"),
        ("silver", "lineitem: quantity strictly positive",
         f"SELECT COUNT(*) FROM {s_lineitem} WHERE l_quantity <= 0"),
        ("silver", "orders: totalprice cannot be negative",
         f"SELECT COUNT(*) FROM {s_orders} WHERE o_totalprice < 0"),
        ("silver", "lineitem: ship_date cannot be earlier than parent order date",
         f"SELECT COUNT(*) FROM {s_lineitem} li JOIN {s_orders} o ON li.l_orderkey = o.o_orderkey WHERE li.l_shipdate < o.o_orderdate"),
        ("silver", "customer: every customer references a valid nation",
         f"SELECT COUNT(*) FROM {s_customer} c LEFT ANTI JOIN {s_nation} n ON c.c_nationkey = n.n_nationkey"),
        ("silver", "supplier: every supplier references a valid nation",
         f"SELECT COUNT(*) FROM {s_supplier} s LEFT ANTI JOIN {s_nation} n ON s.s_nationkey = n.n_nationkey"),
        ("silver", "lineitem: every part-supplier pair exists in partsupp",
         f"SELECT COUNT(*) FROM {s_lineitem} li LEFT ANTI JOIN {s_partsupp} ps ON li.l_partkey = ps.ps_partkey AND li.l_suppkey = ps.ps_suppkey")
    ]

    g_revenue = fq("gold", "revenue_metrics")
    g_customers = fq("gold", "customer_activity")
    g_suppliers = fq("gold", "top_suppliers")
    g_parts = fq("gold", "top_parts")

    checks += [
        ("gold", "reconciliation: gold net revenue matches silver sum within tolerance $0.01",
         f"""SELECT ABS(
                (SELECT COALESCE(SUM(net_revenue), 0) FROM {g_revenue}) - 
                (SELECT COALESCE(SUM(l_extendedprice * (1 - l_discount)), 0) FROM {s_lineitem})
             ) > 0.01 AS is_mismatch FROM (SELECT 1)"""),
        ("gold", "revenue_metrics: net_revenue is strictly positive",
         f"SELECT COUNT(*) FROM {g_revenue} WHERE net_revenue <= 0"),
        ("gold", "top_suppliers: revenue contribution is strictly positive",
         f"SELECT COUNT(*) FROM {g_suppliers} WHERE total_revenue_contribution <= 0"),
        ("gold", "top_parts: revenue contribution is strictly positive",
         f"SELECT COUNT(*) FROM {g_parts} WHERE total_revenue_contribution <= 0"),
        ("gold", "customer_activity: trailing 12m flag has no nulls",
         f"SELECT COUNT(*) FROM {g_customers} WHERE is_active_trailing_12m IS NULL"),
        ("gold", "revenue_metrics: table is not empty",
         f"SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM {g_revenue}")
    ]

    return checks


def run(spark: SparkSession, fail_on_error: bool = False) -> DataFrame:
    """Run all checks and return DataFrame without IPython display calls."""
    rows = [(layer, rule, int(spark.sql(sql).first()[0])) for layer, rule, sql in _checks(spark)]
    df = (spark.createDataFrame(rows, "layer string, rule string, violations long")
          .withColumn("passed", F.col("violations") == 0)
          .withColumn("checked_at", F.current_timestamp()))
    if fail_on_error and df.filter("NOT passed").count():
        raise AssertionError("Validation failed, see the results table")
    return df
