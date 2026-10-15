"""AS IS загрузка demo.* (схема=пользователь demo) из sydafa-oracle-source
в Iceberg ods.oracle_*. См. замечание о стратегии загрузки в load_ods_mssql.py.
"""
import json
import os
import urllib.request

from pyspark.sql import SparkSession

ORACLE_HOST = os.environ.get("ORACLE_HOST", "sydafa-src-oracle-oracle")
ORACLE_PORT = os.environ.get("ORACLE_PORT", "1521")
SYDAFA_URL = os.environ.get("SYDAFA_URL", "http://sydafa:8000")


def _get_source_metadata() -> dict:
    url = f"{SYDAFA_URL}/sources/oracle"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"[load_ods_oracle] Внимание: не удалось получить метаданные от {url} ({e}). Используются дефолтные параметры.")
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

    jdbc_url = f"jdbc:oracle:thin:@//{ORACLE_HOST}:{ORACLE_PORT}/{database}"

    spark = SparkSession.builder.appName("load_ods_oracle").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.ods")

    for tbl in tables:
        schema = tbl["schema"]
        table_name = tbl["name"]
        if table_name.startswith(("_upd_", "_del_")):
            continue
        dbtable = f'"{schema}"."{table_name}"'
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", dbtable)
            .option("user", reader)
            .option("password", reader)
            .option("driver", "oracle.jdbc.OracleDriver")
            .load()
        )
        df.writeTo(f"iceberg.ods.oracle_{table_name}").createOrReplace()
        print(f"[load_ods_oracle] {schema}.{table_name}: {df.count()} строк (под пользователем {reader})")

    spark.stop()



if __name__ == "__main__":
    main()
