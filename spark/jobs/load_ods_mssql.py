"""AS IS загрузка demo.demo.* из mssql-source в Iceberg ods.mssql_*.

Стратегия загрузки (APPEND vs MERGE/overwrite) — открытый вопрос,
см. docs/README.md ("validate_load"). Прототип реализует полный overwrite
таблицы на каждый запуск — простейший вариант, который точно отражает
текущее состояние источника (включая DELETE), но не хранит историю.
"""
import json
import os
import urllib.request

from pyspark.sql import SparkSession

MSSQL_HOST = os.environ.get("MSSQL_HOST", "mssql-source")
MSSQL_PORT = os.environ.get("MSSQL_PORT", "1433")
SYDAFA_URL = os.environ.get("SYDAFA_URL", "http://sydafa:8000")


def _get_source_metadata() -> dict:
    url = f"{SYDAFA_URL}/sources/mssql"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"[load_ods_mssql] Внимание: не удалось получить метаданные от {url} ({e}). Используются дефолтные параметры.")
        return {
            "database": "sydafa",
            "reader": "reader",
            "tables": [
                {"schema": "demo", "name": "customers"},
                {"schema": "demo", "name": "products"},
                {"schema": "demo", "name": "orders"},
            ],
        }


def main() -> None:
    meta = _get_source_metadata()
    database = meta.get("database", "sydafa")
    reader = meta.get("reader", "reader")
    tables = meta.get("tables", [])

    jdbc_url = f"jdbc:sqlserver://{MSSQL_HOST}:{MSSQL_PORT};databaseName={database};encrypt=false"

    spark = SparkSession.builder.appName("load_ods_mssql").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.ods")

    for tbl in tables:
        schema = tbl["schema"]
        table_name = tbl["name"]
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
        df.writeTo(f"iceberg.ods.mssql_{table_name}").createOrReplace()
        print(f"[load_ods_mssql] {schema}.{table_name}: {df.count()} строк (под пользователем {reader})")

    spark.stop()



if __name__ == "__main__":
    main()
