from pyspark.sql import SparkSession, functions as F

from tpch.config import SOURCE, TABLES, fq


def run(spark: SparkSession) -> None:
    for t in TABLES:
        (spark.table(f"{SOURCE}.{t}")
            .withColumn("_ingested_at", F.current_timestamp())
            .write.mode("overwrite").saveAsTable(fq("bronze", t)))


if __name__ == "__main__":
    run(SparkSession.builder.getOrCreate())