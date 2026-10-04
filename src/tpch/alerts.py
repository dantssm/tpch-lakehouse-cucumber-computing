# src/tpch/alerts.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from tpch.config import CATALOG

def check_revenue_drop_alert(spark: SparkSession) -> bool:
    """
    Monitoring mechanism for the CTO and Strategist profile:
    Aggregates weekly net revenue from the Gold layer and detects week-over-week (WoW) 
    drops exceeding 20 percent. Returns True if an alert condition is triggered, 
    otherwise returns False.
    """
    print("Executing revenue drop monitoring query across Gold layer metrics...")
    
    revenue_df = spark.sql(f"""
        SELECT 
            week_start, 
            SUM(net_revenue) AS weekly_revenue
        FROM {CATALOG}.gold.revenue_metrics
        GROUP BY week_start
        ORDER BY week_start
    """)

    window_spec = Window.orderBy("week_start")

    wow_analysis_df = revenue_df \
        .withColumn("previous_week_revenue", F.lag("weekly_revenue", 1).over(window_spec)) \
        .withColumn(
            "wow_change_percentage", 
            (F.col("weekly_revenue") - F.col("previous_week_revenue")) / F.col("previous_week_revenue")
        )

    anomaly_alerts_df = wow_analysis_df.filter(F.col("wow_change_percentage") < -0.20)
    
    anomaly_count = anomaly_alerts_df.count()
    
    if anomaly_count > 0:
        print(f"WARNING: Revenue anomaly detected. Found {anomaly_count} week(s) with a week-over-week drop exceeding 20 percent.")
        display(anomaly_alerts_df)
        return True
    else:
        print("INFO: No revenue drop anomalies detected. All weekly metrics are operating within expected thresholds.")
        return False

if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    check_revenue_drop_alert(spark_session)
