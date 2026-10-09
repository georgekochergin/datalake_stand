"""AS IS загрузка demo.* (схема=пользователь demo) из oracle-source
в Iceberg ods.oracle_*. См. замечание о стратегии загрузки в load_ods_mssql.py.
"""
import os

from pyspark.sql import SparkSession

ORACLE_HOST = os.environ.get("ORACLE_HOST", "oracle-source")
ORACLE_PORT = os.environ.get("ORACLE_PORT", "1521")
ORACLE_SERVICE = os.environ.get("ORACLE_SERVICE", "FREEPDB1")
ORACLE_APP_USER = os.environ.get("ORACLE_APP_USER", "demo")
ORACLE_APP_PASSWORD = os.environ["ORACLE_APP_PASSWORD"]
TABLES = ["customers", "products", "orders"]

JDBC_URL = f"jdbc:oracle:thin:@//{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"


def main() -> None:
    spark = SparkSession.builder.appName("load_ods_oracle").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.ods")

    for table in TABLES:
        df = (
            spark.read.format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", f"{ORACLE_APP_USER}.{table}")
            .option("user", ORACLE_APP_USER)
            .option("password", ORACLE_APP_PASSWORD)
            .option("driver", "oracle.jdbc.OracleDriver")
            .load()
        )
        df.writeTo(f"iceberg.ods.oracle_{table}").createOrReplace()
        print(f"[load_ods_oracle] {table}: {df.count()} строк (AS IS)")

    spark.stop()


if __name__ == "__main__":
    main()
