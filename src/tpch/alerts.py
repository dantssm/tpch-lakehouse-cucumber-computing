# src/tpch/alerts.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from .config import CATALOG

def run(spark: SparkSession) -> bool:
    """
    Monitoring and anomaly detection:
    Checks week-over-week (WoW) net revenue drops exceeding 20 percent.
    Excludes the first incomplete period per project README.
    Returns True if an anomaly is triggered, otherwise returns False.
    """
    print("Executing revenue drop anomaly check across Gold layer...")
    
    revenue_df = spark.sql(f"""
        SELECT 
            week_start, 
            SUM(net_revenue) AS weekly_revenue
        FROM {CATALOG}.gold.revenue_metrics
        GROUP BY week_start
        ORDER BY week_start
    """)

    min_week = revenue_df.agg(F.min("week_start")).collect()[0][0]
    filtered_revenue_df = revenue_df.filter(F.col("week_start") != min_week)

    window_spec = Window.orderBy("week_start")
    
    wow_analysis_df = filtered_revenue_df \
        .withColumn("previous_week_revenue", F.lag("weekly_revenue", 1).over(window_spec)) \
        .withColumn(
            "wow_change_percentage", 
            (F.col("weekly_revenue") - F.col("previous_week_revenue")) / F.col("previous_week_revenue")
        )

    anomaly_alerts_df = wow_analysis_df.filter(F.col("wow_change_percentage") < -0.20)
    anomaly_count = anomaly_alerts_df.count()
    
    if anomaly_count > 0:
        print(f"WARNING: Revenue anomaly detected. Found {anomaly_count} week(s) with WoW drop exceeding 20 percent.")
        anomaly_alerts_df.show(truncate=False)
        return True
    else:
        print("INFO: No revenue anomalies detected. All metrics are operating within expected thresholds.")
        return False

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    run(spark)
