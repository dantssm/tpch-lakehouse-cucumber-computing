from pyspark.sql import SparkSession
from tpch.config import CATALOG, AS_OF_DATE

def run(spark: SparkSession) -> None:

    #Дохід (Revenue Metrics)
    spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.gold.revenue_metrics AS
        SELECT
            o.o_orderdate AS date,
            date_trunc('week', o.o_orderdate) AS week_start,
            date_trunc('quarter', o.o_orderdate) AS quarter_start,
            SUM(l.l_extendedprice * (1 - l.l_discount)) AS net_revenue
        FROM {CATALOG}.silver.orders o
        JOIN {CATALOG}.silver.lineitem l ON o.o_orderkey = l.l_orderkey
        GROUP BY 1, 2, 3
    """)

    #Активність клієнтів (Customer Activity) за останні 12 місяців
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.gold.customer_activity AS
        SELECT
            c.c_custkey,
            c.c_name,
            MAX(o.o_orderdate) AS last_order_date,
            CASE 
                WHEN MAX(o.o_orderdate) >= date_sub('{AS_OF_DATE}', 365) 
                 AND MAX(o.o_orderdate) <= '{AS_OF_DATE}' 
                THEN TRUE 
                ELSE FALSE 
            END AS is_active_trailing_12m
        FROM {CATALOG}.silver.customer c
        LEFT JOIN {CATALOG}.silver.orders o ON c.c_custkey = o.o_custkey
        GROUP BY 1, 2
    """)

    #Топ контриб'ютори (Постачальники)
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.gold.top_suppliers AS
        SELECT
            s.s_suppkey,
            s.s_name,
            SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue_contribution
        FROM {CATALOG}.silver.supplier s
        JOIN {CATALOG}.silver.lineitem l ON s.s_suppkey = l.l_suppkey
        GROUP BY 1, 2
    """)

    #Топ контриб'ютори (Продукти)
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.gold.top_parts AS
        SELECT
            p.p_partkey,
            p.p_name,
            SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue_contribution
        FROM {CATALOG}.silver.part p
        JOIN {CATALOG}.silver.lineitem l ON p.p_partkey = l.l_partkey
        GROUP BY 1, 2
    """)

if __name__ == "__main__":
    run(SparkSession.builder.getOrCreate())