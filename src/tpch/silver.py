from pyspark.sql import SparkSession


def run(spark: SparkSession) -> None:

    # 1. REGION
    spark.sql("""
        CREATE OR REPLACE TABLE workspace.silver.quarantine_region AS
        SELECT
            r_regionkey,
            r_name,
            r_comment,
            _ingested_at,
            CASE 
                WHEN r_regionkey IS NULL THEN 'NULL value in primary key: r_regionkey'
                ELSE 'Unknown error'
            END AS rejection_reason
        FROM workspace.bronze.region
        WHERE r_regionkey IS NULL;
    """)

    spark.sql("""
        CREATE OR REPLACE TABLE workspace.silver.region AS
        SELECT
            CAST(r_regionkey AS BIGINT) AS r_regionkey,
            TRIM(r_name) AS r_name,
            TRIM(r_comment) AS r_comment,
            _ingested_at
        FROM workspace.bronze.region
        WHERE r_regionkey IS NOT NULL;
    """)

    spark.sql("ALTER TABLE workspace.silver.region ALTER COLUMN r_regionkey SET NOT NULL;")
    spark.sql("ALTER TABLE workspace.silver.region ADD CONSTRAINT pk_region PRIMARY KEY (r_regionkey);")


    # 2. NATION
    spark.sql("""
        CREATE OR REPLACE TABLE workspace.silver.quarantine_nation AS
        SELECT
            n_nationkey,
            n_name,
            n_regionkey,
            n_comment,
            _ingested_at,
            CASE 
                WHEN n_nationkey IS NULL THEN 'NULL value in primary key: n_nationkey'
                WHEN n_nationkey < 0 THEN 'Invalid primary key: n_nationkey must be non-negative'
                WHEN n_regionkey IS NULL THEN 'NULL value in foreign key: n_regionkey'
                WHEN n_name IS NULL OR TRIM(n_name) = '' THEN 'Missing or empty nation name: n_name'
                ELSE 'Unknown error'
            END AS rejection_reason
        FROM workspace.bronze.nation
        WHERE n_nationkey IS NULL 
           OR n_nationkey < 0 
           OR n_regionkey IS NULL 
           OR n_name IS NULL 
           OR TRIM(n_name) = '';
    """)

    spark.sql("""
        CREATE OR REPLACE TABLE workspace.silver.nation AS
        SELECT
            CAST(n_nationkey AS BIGINT) AS n_nationkey,
            TRIM(n_name) AS n_name,
            CAST(n_regionkey AS BIGINT) AS n_regionkey,
            TRIM(n_comment) AS n_comment,
            _ingested_at
        FROM workspace.bronze.nation
        WHERE n_nationkey IS NOT NULL
          AND n_nationkey >= 0
          AND n_regionkey IS NOT NULL
          AND n_name IS NOT NULL
          AND TRIM(n_name) != '';
    """)

    spark.sql("ALTER TABLE workspace.silver.nation ALTER COLUMN n_nationkey SET NOT NULL;")
    spark.sql("ALTER TABLE workspace.silver.nation ALTER COLUMN n_regionkey SET NOT NULL;")
    spark.sql("ALTER TABLE workspace.silver.nation ADD CONSTRAINT pk_nation PRIMARY KEY (n_nationkey);")
    spark.sql("ALTER TABLE workspace.silver.nation ADD CONSTRAINT fk_nation_region FOREIGN KEY (n_regionkey) REFERENCES workspace.silver.region(r_regionkey);")


    # 3. CUSTOMER
    spark.sql("""
        CREATE OR REPLACE TABLE workspace.silver.quarantine_customer AS
        SELECT
            c_custkey,
            c_name,
            c_address,
            c_nationkey,
            c_phone,
            c_acctbal,
            c_mktsegment,
            c_comment,
            _ingested_at,
            CASE 
                WHEN c_custkey IS NULL THEN 'NULL value in primary key: c_custkey'
                WHEN c_nationkey IS NULL THEN 'NULL value in foreign key: c_nationkey'
                ELSE 'Unknown error'
            END AS rejection_reason
        FROM workspace.bronze.customer
        WHERE c_custkey IS NULL 
           OR c_nationkey IS NULL;
    """)

    spark.sql("""
        CREATE OR REPLACE TABLE workspace.silver.customer AS
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
        FROM workspace.bronze.customer
        WHERE c_custkey IS NOT NULL
          AND c_nationkey IS NOT NULL;
    """)

    spark.sql("ALTER TABLE workspace.silver.customer ALTER COLUMN c_custkey SET NOT NULL;")
    spark.sql("ALTER TABLE workspace.silver.customer ALTER COLUMN c_nationkey SET NOT NULL;")
    spark.sql("ALTER TABLE workspace.silver.customer ADD CONSTRAINT pk_customer PRIMARY KEY (c_custkey);")
    spark.sql("ALTER TABLE workspace.silver.customer ADD CONSTRAINT fk_customer_nation FOREIGN KEY (c_nationkey) REFERENCES workspace.silver.nation(n_nationkey);")


    # 4. ORDERS
    spark.sql("""
        CREATE OR REPLACE TABLE workspace.silver.quarantine_orders AS
        SELECT
            o_orderkey,
            o_custkey,
            o_orderstatus,
            o_totalprice,
            o_orderdate,
            o_orderpriority,
            o_clerk,
            o_shippriority,
            o_comment,
            _ingested_at,
            CASE 
                WHEN o_orderkey IS NULL THEN 'NULL value in primary key: o_orderkey'
                WHEN o_custkey IS NULL THEN 'NULL value in foreign key: o_custkey'
                WHEN o_totalprice < 0 THEN 'Negative total price'
                ELSE 'Unknown error'
            END AS rejection_reason
        FROM workspace.bronze.orders
        WHERE o_orderkey IS NULL 
           OR o_custkey IS NULL
           OR o_totalprice < 0;
    """)

    spark.sql("""
        CREATE OR REPLACE TABLE workspace.silver.orders AS
        SELECT
            CAST(o_orderkey AS BIGINT) AS o_orderkey,
            CAST(o_custkey AS BIGINT) AS o_custkey,
            TRIM(o_orderstatus) AS o_orderstatus,
            CAST(o_totalprice AS DECIMAL(18,2)) AS o_totalprice,
            CAST(o_orderdate AS DATE) AS o_orderdate,
            TRIM(o_orderpriority) AS o_orderpriority,
            TRIM(o_clerk) AS o_clerk,
            CAST(TRIM(o_shippriority) AS INT) AS o_shippriority,
            TRIM(o_comment) AS o_comment,
            _ingested_at
        FROM workspace.bronze.orders
        WHERE o_orderkey IS NOT NULL
          AND o_custkey IS NOT NULL
          AND o_totalprice >= 0;
    """)

    spark.sql("ALTER TABLE workspace.silver.orders ALTER COLUMN o_orderkey SET NOT NULL;")
    spark.sql("ALTER TABLE workspace.silver.orders ALTER COLUMN o_custkey SET NOT NULL;")
    spark.sql("ALTER TABLE workspace.silver.orders ADD CONSTRAINT pk_orders PRIMARY KEY (o_orderkey);")
    spark.sql("ALTER TABLE workspace.silver.orders ADD CONSTRAINT fk_orders_customer FOREIGN KEY (o_custkey) REFERENCES workspace.silver.customer(c_custkey);")


    # 5. PART
    spark.sql("""
        CREATE OR REPLACE TABLE workspace.silver.quarantine_part AS
        SELECT
            p_partkey,
            p_name,
            p_mfgr,
            p_brand,
            p_type,
            p_size,
            p_container,
            p_retailprice,
            p_comment,
            _ingested_at,
            CASE 
                WHEN p_partkey IS NULL THEN 'NULL value in primary key: p_partkey'
                WHEN p_retailprice < 0 THEN 'Negative retail price: p_retailprice'
                WHEN p_size < 0 THEN 'Negative size: p_size'
                ELSE 'Unknown error'
            END AS rejection_reason
        FROM workspace.bronze.part
        WHERE p_partkey IS NULL
           OR p_retailprice < 0
           OR p_size < 0;
    """)

    spark.sql("""
        CREATE OR REPLACE TABLE workspace.silver.part AS
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
        FROM workspace.bronze.part
        WHERE p_partkey IS NOT NULL
          AND p_retailprice >= 0
          AND p_size >= 0;
    """)

    spark.sql("ALTER TABLE workspace.silver.part ALTER COLUMN p_partkey SET NOT NULL;")
    spark.sql("ALTER TABLE workspace.silver.part ADD CONSTRAINT pk_part PRIMARY KEY (p_partkey);")
    spark.sql("ALTER TABLE workspace.silver.part ADD CONSTRAINT chk_part_retailprice CHECK (p_retailprice >= 0);")
    spark.sql("ALTER TABLE workspace.silver.part ADD CONSTRAINT chk_part_size CHECK (p_size >= 0);")


    # 6. SUPPLIER
    spark.sql("""
        CREATE OR REPLACE TABLE workspace.silver.quarantine_supplier AS
        SELECT
            s_suppkey,
            s_name,
            s_address,
            s_nationkey,
            s_phone,
            s_acctbal,
            s_comment,
            _ingested_at,
            CASE 
                WHEN s_suppkey IS NULL THEN 'NULL value in primary key: s_suppkey'
                WHEN s_nationkey IS NULL THEN 'NULL value in foreign key: s_nationkey'
                ELSE 'Unknown error'
            END AS rejection_reason
        FROM workspace.bronze.supplier
        WHERE s_suppkey IS NULL
           OR s_nationkey IS NULL;
    """)

    spark.sql("""
        CREATE OR REPLACE TABLE workspace.silver.supplier AS
        SELECT
            CAST(s_suppkey AS BIGINT) AS s_suppkey,
            TRIM(s_name) AS s_name,
            TRIM(s_address) AS s_address,
            CAST(s_nationkey AS BIGINT) AS s_nationkey,
            TRIM(s_phone) AS s_phone,
            CAST(s_acctbal AS DECIMAL(18,2)) AS s_acctbal,
            TRIM(s_comment) AS s_comment,
            _ingested_at
        FROM workspace.bronze.supplier
        WHERE s_suppkey IS NOT NULL
          AND s_nationkey IS NOT NULL;
    """)

    spark.sql("ALTER TABLE workspace.silver.supplier ALTER COLUMN s_suppkey SET NOT NULL;")
    spark.sql("ALTER TABLE workspace.silver.supplier ALTER COLUMN s_nationkey SET NOT NULL;")
    spark.sql("ALTER TABLE workspace.silver.supplier ADD CONSTRAINT pk_supplier PRIMARY KEY (s_suppkey);")
    spark.sql("ALTER TABLE workspace.silver.supplier ADD CONSTRAINT fk_supplier_nation FOREIGN KEY (s_nationkey) REFERENCES workspace.silver.nation(n_nationkey);")


    # 7. PARTSUPP
    spark.sql("""
        CREATE OR REPLACE TABLE workspace.silver.quarantine_partsupp AS
        SELECT
            ps_partkey,
            ps_suppkey,
            ps_availqty,
            ps_supplycost,
            ps_comment,
            _ingested_at,
            CASE 
                WHEN ps_partkey IS NULL OR ps_suppkey IS NULL THEN 'NULL value in composite primary key (ps_partkey or ps_suppkey)'
                WHEN ps_availqty < 0 THEN 'Negative available quantity: ps_availqty'
                WHEN ps_supplycost < 0 THEN 'Negative supply cost: ps_supplycost'
                ELSE 'Unknown error'
            END AS rejection_reason
        FROM workspace.bronze.partsupp
        WHERE ps_partkey IS NULL
           OR ps_suppkey IS NULL
           OR ps_availqty < 0
           OR ps_supplycost < 0;
    """)

    spark.sql("""
        CREATE OR REPLACE TABLE workspace.silver.partsupp AS
        SELECT
            CAST(ps_partkey AS BIGINT) AS ps_partkey,
            CAST(ps_suppkey AS BIGINT) AS ps_suppkey,
            CAST(ps_availqty AS INT) AS ps_availqty,
            CAST(ps_supplycost AS DECIMAL(18,2)) AS ps_supplycost,
            TRIM(ps_comment) AS ps_comment,
            _ingested_at
        FROM workspace.bronze.partsupp
        WHERE ps_partkey IS NOT NULL
          AND ps_suppkey IS NOT NULL
          AND ps_availqty >= 0
          AND ps_supplycost >= 0;
    """)

    spark.sql("ALTER TABLE workspace.silver.partsupp ALTER COLUMN ps_partkey SET NOT NULL;")
    spark.sql("ALTER TABLE workspace.silver.partsupp ALTER COLUMN ps_suppkey SET NOT NULL;")
    spark.sql("ALTER TABLE workspace.silver.partsupp ADD CONSTRAINT pk_partsupp PRIMARY KEY (ps_partkey, ps_suppkey);")
    spark.sql("ALTER TABLE workspace.silver.partsupp ADD CONSTRAINT fk_partsupp_part FOREIGN KEY (ps_partkey) REFERENCES workspace.silver.part(p_partkey);")
    spark.sql("ALTER TABLE workspace.silver.partsupp ADD CONSTRAINT fk_partsupp_supplier FOREIGN KEY (ps_suppkey) REFERENCES workspace.silver.supplier(s_suppkey);")
    spark.sql("ALTER TABLE workspace.silver.partsupp ADD CONSTRAINT chk_partsupp_availqty CHECK (ps_availqty >= 0);")
    spark.sql("ALTER TABLE workspace.silver.partsupp ADD CONSTRAINT chk_partsupp_supplycost CHECK (ps_supplycost >= 0);")


    # 8. LINEITEM
    spark.sql("""
        CREATE OR REPLACE TABLE workspace.silver.quarantine_lineitem AS
        SELECT
            l_orderkey,
            l_partkey,
            l_suppkey,
            l_linenumber,
            l_quantity,
            l_extendedprice,
            l_discount,
            l_tax,
            l_returnflag,
            l_linestatus,
            l_shipdate,
            l_commitdate,
            l_receiptdate,
            l_shipinstruct,
            l_shipmode,
            l_comment,
            _ingested_at,
            CASE 
                WHEN l_orderkey IS NULL OR l_linenumber IS NULL THEN 'NULL value in composite primary key (l_orderkey or l_linenumber)'
                WHEN l_partkey IS NULL OR l_suppkey IS NULL THEN 'NULL value in foreign key columns (l_partkey or l_suppkey)'
                WHEN CAST(l_discount AS DECIMAL(18,2)) < 0 OR CAST(l_discount AS DECIMAL(18,2)) > 0.10 THEN 'Discount out of allowed range (0.00 - 0.10)'
                WHEN CAST(l_tax AS DECIMAL(18,2)) < 0 OR CAST(l_tax AS DECIMAL(18,2)) > 0.08 THEN 'Tax out of allowed range (0.00 - 0.08)'
                WHEN CAST(l_quantity AS DECIMAL(18,2)) < 0 THEN 'Negative quantity'
                WHEN CAST(l_extendedprice AS DECIMAL(18,2)) < 0 THEN 'Negative extended price'
                ELSE 'Unknown error'
            END AS rejection_reason
        FROM workspace.bronze.lineitem
        WHERE l_orderkey IS NULL 
           OR l_linenumber IS NULL
           OR l_partkey IS NULL 
           OR l_suppkey IS NULL
           OR CAST(l_discount AS DECIMAL(18,2)) < 0 OR CAST(l_discount AS DECIMAL(18,2)) > 0.10
           OR CAST(l_tax AS DECIMAL(18,2)) < 0 OR CAST(l_tax AS DECIMAL(18,2)) > 0.08
           OR CAST(l_quantity AS DECIMAL(18,2)) < 0
           OR CAST(l_extendedprice AS DECIMAL(18,2)) < 0;
    """)

    spark.sql("""
        CREATE OR REPLACE TABLE workspace.silver.lineitem AS
        SELECT
            CAST(l_orderkey AS BIGINT) AS l_orderkey,
            CAST(l_partkey AS BIGINT) AS l_partkey,
            CAST(l_suppkey AS BIGINT) AS l_suppkey,
            CAST(l_linenumber AS INT) AS l_linenumber,
            CAST(l_quantity AS DECIMAL(18,2)) AS l_quantity,
            CAST(l_extendedprice AS DECIMAL(18,2)) AS l_extendedprice,
            CAST(l_discount AS DECIMAL(4,2)) AS l_discount,
            CAST(l_tax AS DECIMAL(4,2)) AS l_tax,
            TRIM(l_returnflag) AS l_returnflag,
            TRIM(l_linestatus) AS l_linestatus,
            CAST(l_shipdate AS DATE) AS l_shipdate,
            CAST(l_commitdate AS DATE) AS l_commitdate,
            CAST(l_receiptdate AS DATE) AS l_receiptdate,
            TRIM(l_shipinstruct) AS l_shipinstruct,
            TRIM(l_shipmode) AS l_shipmode,
            TRIM(l_comment) AS l_comment,
            _ingested_at
        FROM workspace.bronze.lineitem
        WHERE l_orderkey IS NOT NULL
          AND l_linenumber IS NOT NULL
          AND l_partkey IS NOT NULL
          AND l_suppkey IS NOT NULL
          AND CAST(l_discount AS DECIMAL(18,2)) BETWEEN 0 AND 0.10
          AND CAST(l_tax AS DECIMAL(18,2)) BETWEEN 0 AND 0.08
          AND CAST(l_quantity AS DECIMAL(18,2)) >= 0
          AND CAST(l_extendedprice AS DECIMAL(18,2)) >= 0;
    """)

    spark.sql("ALTER TABLE workspace.silver.lineitem ALTER COLUMN l_orderkey SET NOT NULL;")
    spark.sql("ALTER TABLE workspace.silver.lineitem ALTER COLUMN l_linenumber SET NOT NULL;")
    spark.sql("ALTER TABLE workspace.silver.lineitem ALTER COLUMN l_partkey SET NOT NULL;")
    spark.sql("ALTER TABLE workspace.silver.lineitem ALTER COLUMN l_suppkey SET NOT NULL;")
    spark.sql("ALTER TABLE workspace.silver.lineitem ADD CONSTRAINT pk_lineitem PRIMARY KEY (l_orderkey, l_linenumber);")
    spark.sql("ALTER TABLE workspace.silver.lineitem ADD CONSTRAINT fk_lineitem_orders FOREIGN KEY (l_orderkey) REFERENCES workspace.silver.orders(o_orderkey);")
    spark.sql("ALTER TABLE workspace.silver.lineitem ADD CONSTRAINT fk_lineitem_partsupp FOREIGN KEY (l_partkey, l_suppkey) REFERENCES workspace.silver.partsupp(ps_partkey, ps_suppkey);")
    spark.sql("ALTER TABLE workspace.silver.lineitem ADD CONSTRAINT chk_lineitem_discount CHECK (l_discount >= 0.00 AND l_discount <= 0.10);")
    spark.sql("ALTER TABLE workspace.silver.lineitem ADD CONSTRAINT chk_lineitem_tax CHECK (l_tax >= 0.00 AND l_tax <= 0.08);")


if __name__ == "__main__":
    run(SparkSession.builder.getOrCreate())