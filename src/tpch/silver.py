from pyspark.sql import SparkSession

from tpch.config import CATALOG


def run(spark: SparkSession) -> None:

    # 1. REGION
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.silver.region AS
        SELECT
            CAST(r_regionkey AS BIGINT) AS r_regionkey,
            TRIM(r_name) AS r_name,
            TRIM(r_comment) AS r_comment
        FROM {CATALOG}.bronze.region
        WHERE r_regionkey IS NOT NULL
    """)

    # 2. NATION
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.silver.nation AS
        SELECT
            CAST(n_nationkey AS BIGINT) AS n_nationkey,
            TRIM(n_name) AS n_name,
            CAST(n_regionkey AS BIGINT) AS n_regionkey,
            TRIM(n_comment) AS n_comment
        FROM {CATALOG}.bronze.nation
        WHERE n_nationkey IS NOT NULL
          AND n_regionkey IS NOT NULL
    """)

    # 3. CUSTOMER
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.silver.customer AS
        SELECT
            CAST(c_custkey AS BIGINT) AS c_custkey,
            TRIM(c_name) AS c_name,
            TRIM(c_address) AS c_address,
            CAST(c_nationkey AS BIGINT) AS c_nationkey,
            TRIM(c_phone) AS c_phone,
            CAST(c_acctbal AS DECIMAL(18,2)) AS c_acctbal,
            TRIM(c_mktsegment) AS c_mktsegment,
            TRIM(c_comment) AS c_comment,
            _ingested_at
        FROM {CATALOG}.bronze.customer
        WHERE c_custkey IS NOT NULL
          AND c_nationkey IS NOT NULL
    """)

    # 4. ORDERS
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.silver.orders AS
        SELECT
            CAST(o_orderkey AS BIGINT) AS o_orderkey,
            CAST(o_custkey AS BIGINT) AS o_custkey,
            TRIM(o_orderstatus) AS o_orderstatus,
            CAST(o_totalprice AS DECIMAL(18,2)) AS o_totalprice,
            CAST(o_orderdate AS DATE) AS o_orderdate,
            TRIM(o_orderpriority) AS o_orderpriority,
            TRIM(o_clerk) AS o_clerk,
            TRIM(o_shippriority) AS o_shippriority,
            TRIM(o_comment) AS o_comment,
            _ingested_at
        FROM {CATALOG}.bronze.orders
        WHERE o_orderkey IS NOT NULL
          AND o_custkey IS NOT NULL
    """)

    # 5. PART
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.silver.part AS
        SELECT
            CAST(p_partkey AS BIGINT) AS p_partkey,
            TRIM(p_name) AS p_name,
            TRIM(p_mfgr) AS p_mfgr,
            TRIM(p_brand) AS p_brand,
            TRIM(p_type) AS p_type,
            CAST(p_size AS INT) AS p_size,
            TRIM(p_container) AS p_container,
            CAST(p_retailprice AS DECIMAL(18,2)) AS p_retailprice,
            TRIM(p_comment) AS p_comment,
            _ingested_at
        FROM {CATALOG}.bronze.part
        WHERE p_partkey IS NOT NULL
    """)

    # 6. SUPPLIER
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.silver.supplier AS
        SELECT
            CAST(s_suppkey AS BIGINT) AS s_suppkey,
            TRIM(s_name) AS s_name,
            TRIM(s_address) AS s_address,
            CAST(s_nationkey AS BIGINT) AS s_nationkey,
            TRIM(s_phone) AS s_phone,
            CAST(s_acctbal AS DECIMAL(18,2)) AS s_acctbal,
            TRIM(s_comment) AS s_comment,
            _ingested_at
        FROM {CATALOG}.bronze.supplier
        WHERE s_suppkey IS NOT NULL
          AND s_nationkey IS NOT NULL
    """)

    # 7. PARTSUPP
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.silver.partsupp AS
        SELECT
            CAST(ps_partkey AS BIGINT) AS ps_partkey,
            CAST(ps_suppkey AS BIGINT) AS ps_suppkey,
            CAST(ps_availqty AS INT) AS ps_availqty,
            CAST(ps_supplycost AS DECIMAL(18,2)) AS ps_supplycost,
            TRIM(ps_comment) AS ps_comment,
            _ingested_at
        FROM {CATALOG}.bronze.partsupp
        WHERE ps_partkey IS NOT NULL
          AND ps_suppkey IS NOT NULL
    """)

    # 8. LINEITEM
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.silver.lineitem AS
        SELECT
            CAST(l_orderkey AS BIGINT) AS l_orderkey,
            CAST(l_partkey AS BIGINT) AS l_partkey,
            CAST(l_suppkey AS BIGINT) AS l_suppkey,
            CAST(l_linenumber AS INT) AS l_linenumber,
            CAST(l_quantity AS DECIMAL(18,2)) AS l_quantity,
            CAST(l_extendedprice AS DECIMAL(18,2)) AS l_extendedprice,
            CAST(l_discount AS DECIMAL(18,2)) AS l_discount,
            CAST(l_tax AS DECIMAL(18,2)) AS l_tax,
            TRIM(l_returnflag) AS l_returnflag,
            TRIM(l_linestatus) AS l_linestatus,
            CAST(l_shipdate AS DATE) AS l_shipdate,
            CAST(l_commitdate AS DATE) AS l_commitdate,
            CAST(l_receiptdate AS DATE) AS l_receiptdate,
            TRIM(l_shipinstruct) AS l_shipinstruct,
            TRIM(l_shipmode) AS l_shipmode,
            TRIM(l_comment) AS l_comment,
            _ingested_at
        FROM {CATALOG}.bronze.lineitem
        WHERE l_orderkey IS NOT NULL
          AND l_partkey IS NOT NULL
          AND l_suppkey IS NOT NULL
          AND l_linenumber IS NOT NULL
    """)


if __name__ == "__main__":
    run(SparkSession.builder.getOrCreate())