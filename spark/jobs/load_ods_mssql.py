"""AS IS загрузка demo.demo.* из mssql-source в Iceberg ods.mssql_*.

Стратегия загрузки (APPEND vs MERGE/overwrite) — открытый вопрос,
см. docs/PLAN.md ("validate_load"). Прототип реализует полный overwrite
таблицы на каждый запуск — простейший вариант, который точно отражает
текущее состояние источника (включая DELETE), но не хранит историю.
"""
import os

from pyspark.sql import SparkSession

MSSQL_HOST = os.environ.get("MSSQL_HOST", "mssql-source")
MSSQL_PORT = os.environ.get("MSSQL_PORT", "1433")
MSSQL_PASSWORD = os.environ["MSSQL_SA_PASSWORD"]
TABLES = ["customers", "products", "orders"]

JDBC_URL = f"jdbc:sqlserver://{MSSQL_HOST}:{MSSQL_PORT};databaseName=demo;encrypt=false"


def main() -> None:
    spark = SparkSession.builder.appName("load_ods_mssql").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.ods")

    for table in TABLES:
        df = (
            spark.read.format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", f"demo.{table}")
            .option("user", "sa")
            .option("password", MSSQL_PASSWORD)
            .load()
        )
        df.writeTo(f"iceberg.ods.mssql_{table}").createOrReplace()
        print(f"[load_ods_mssql] {table}: {df.count()} строк (AS IS)")

    spark.stop()


if __name__ == "__main__":
    main()
