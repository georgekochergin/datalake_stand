"""build_marts_demo: демонстрационная трансформация через Spark (заглушка).

Содержание сознательно не привязано к реальной бизнес-логике (см.
docs/README.md) — показывает только механику "трансформация через Spark"
в паре с build_mart_trino_demo.sql ("через Trino SQL"), без требования
идентичности результата между движками.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    spark = SparkSession.builder.appName("build_mart_spark_demo").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.mart_spark")

    orders = spark.table("iceberg.ods.mssql_orders")
    demo_mart = orders.groupBy("customer_id").agg(
        F.count("*").alias("orders_count"),
        F.sum("amount").alias("total_amount"),
    )
    demo_mart.writeTo("iceberg.mart_spark.customer_totals").createOrReplace()
    print(f"[build_mart_spark_demo] customer_totals: {demo_mart.count()} строк")

    spark.stop()


if __name__ == "__main__":
    main()
