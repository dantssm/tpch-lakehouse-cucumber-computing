# src/tpch/orchestrator.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from tpch.config import CATALOG
from . import bronze, silver, gold, validation, alerts

def run_full_pipeline(spark: SparkSession) -> None:
    """
    Master pipeline orchestrator:
    Sequentially executes Bronze ingestion, Silver 3NF transformation,
    Gold data mart aggregation, Data Quality validation, and anomaly alerts.
    Stops execution and raises an exception if any critical step fails.
    """
    print("=" * 60)
    print("STARTING TPC-H LAKEHOUSE PIPELINE EXECUTION")
    print(f"Target Catalog: {CATALOG}")
    print("=" * 60)

    try:
        print("\n[Step 1/5] Initializing Lakehouse catalog schemas...")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
        print("SUCCESS: Schemas verified and ready.")

        print("\n[Step 2/5] Executing Bronze layer ingestion...")
        bronze.run(spark)
        print("SUCCESS: Bronze layer ingestion completed.")

        print("\n[Step 3/5] Executing Silver layer 3NF transformation...")
        silver.run(spark)
        print("SUCCESS: Silver layer transformation completed.")

        print("\n[Step 4/5] Executing Gold layer data mart aggregation...")
        gold.run(spark)
        print("SUCCESS: Gold layer aggregation completed.")

        print("\n[Step 5/5] Executing Data Quality validation and monitoring...")
        val_df = validation.run(spark, fail_on_error=False)
        display(val_df)
        
        failed_checks = val_df.filter("passed = false").count()
        if failed_checks > 0:
            print(f"WARNING: Validation completed with {failed_checks} failing rule(s).")
        else:
            print("SUCCESS: All Data Quality validation rules passed successfully.")

        try:
            if hasattr(alerts, "run"):
                alerts.run(spark)
            elif hasattr(alerts, "check_revenue_drop_alert"):
                alerts.check_revenue_drop_alert(spark)
            else:
                print("INFO: Executing inline revenue drop anomaly check...")
                revenue_df = spark.sql(f"""
                    SELECT week_start, SUM(net_revenue) AS weekly_revenue
                    FROM {CATALOG}.gold.revenue_metrics
                    GROUP BY week_start
                    ORDER BY week_start
                """)
                window_spec = Window.orderBy("week_start")
                wow_df = revenue_df \
                    .withColumn("prev_rev", F.lag("weekly_revenue", 1).over(window_spec)) \
                    .withColumn("change", (F.col("weekly_revenue") - F.col("prev_rev")) / F.col("prev_rev"))
                anomalies = wow_df.filter(F.col("change") < -0.20)
                if anomalies.count() > 0:
                    print(f"WARNING: Found {anomalies.count()} week(s) with WoW revenue drop > 20%.")
                    display(anomalies)
                else:
                    print("INFO: No revenue anomalies detected.")
        except Exception as alert_err:
            print(f"WARNING: Alert module execution skipped due to: {str(alert_err)}")

        print("\n" + "=" * 60)
        print("PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
        print("=" * 60)

    except Exception as e:
        print("\n" + "!" * 60)
        print(f"PIPELINE EXECUTION FAILED: {str(e)}")
        print("!" * 60)
        raise e

if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    run_full_pipeline(spark_session)
