"""AS IS загрузка demo.* (схема demo) из sydafa-src-mssql-db2
в Iceberg ods.db2_*.
"""
import json
import os
import urllib.request

from pyspark.sql import SparkSession

MSSQL_HOST = os.environ.get("MSSQL_HOST", "sydafa-src-mssql-db2")
MSSQL_PORT = os.environ.get("MSSQL_PORT", "1433")
SYDAFA_URL = os.environ.get("SYDAFA_URL", "http://sydafa:8000")


def _get_source_metadata() -> dict:
    url = f"{SYDAFA_URL}/sources/db2"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"[load_ods_db2] Внимание: не удалось получить метаданные от {url} ({e}). Используются дефолтные параметры.")
        return {
            "database": "sydafa",
            "reader": "sydafa",
            "tables": [
                {"schema": "demo", "name": "customers"},
                {"schema": "demo", "name": "products"},
                {"schema": "demo", "name": "orders"},
            ],
        }


def main() -> None:
    meta = _get_source_metadata()
    database = meta.get("database", "sydafa")
    reader = meta.get("reader", "sydafa")
    tables = meta.get("tables", [])

    jdbc_url = f"jdbc:sqlserver://{MSSQL_HOST}:{MSSQL_PORT};databaseName={database};encrypt=false"

    spark = SparkSession.builder.appName("load_ods_db2").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.ods")

    for tbl in tables:
        schema = tbl["schema"]
        table_name = tbl["name"]
        if table_name.startswith(("_upd_", "_del_")):
            continue
        dbtable = f"[{schema}].[{table_name}]"
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", dbtable)
            .option("user", reader)
            .option("password", reader)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load()
        )
        df.writeTo(f"iceberg.ods.db2_{table_name.lower()}").createOrReplace()
        print(f"[load_ods_db2] {schema}.{table_name}: {df.count()} строк (под пользователем {reader})")

    spark.stop()


if __name__ == "__main__":
    main()
