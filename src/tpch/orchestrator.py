# src/tpch/orchestrator.py
from pyspark.sql import SparkSession
from .config import CATALOG, ensure_schemas
from . import bronze, silver, gold, validation, alerts

def run_full_pipeline(spark: SparkSession, run_bronze: bool = True) -> None:
    """
    Master pipeline orchestrator:
    Sequentially executes Bronze ingestion (optional), Silver 3NF transformation,
    Gold data mart aggregation, Data Quality validation, and anomaly alerts.
    """
    print("=" * 60)
    print("STARTING TPC-H LAKEHOUSE PIPELINE EXECUTION")
    print(f"Target Catalog: {CATALOG}")
    print("=" * 60)

    try:

        print("\n[Step 1/5] Ensuring Lakehouse catalog schemas exist...")
        ensure_schemas(spark)
        print("SUCCESS: Schemas verified and ready.")

        if run_bronze:
            print("\n[Step 2/5] Executing Bronze layer ingestion...")
            bronze.run(spark)
            print("SUCCESS: Bronze layer ingestion completed.")
        else:
            print("\n[Step 2/5] Skipping Bronze layer ingestion (run_bronze=False).")

        print("\n[Step 3/5] Executing Silver layer 3NF transformation...")
        silver.run(spark)
        print("SUCCESS: Silver layer transformation completed.")

        print("\n[Step 4/5] Executing Gold layer data mart aggregation...")
        gold.run(spark)
        print("SUCCESS: Gold layer aggregation completed.")

        print("\n[Step 5/5] Executing Data Quality validation and monitoring...")
        val_df = validation.run(spark, fail_on_error=False)
        
        failed_checks = val_df.filter("passed = false").count()
        if failed_checks > 0:
            print(f"WARNING: Validation completed with {failed_checks} failing rule(s).")
            val_df.filter("passed = false").show(truncate=False)
        else:
            print("SUCCESS: All Data Quality validation rules passed successfully.")

        alerts.run(spark)

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
