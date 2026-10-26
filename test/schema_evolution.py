"""Exercise raw Iceberg schema evolution using the actual loader DDL helper."""

import importlib.util

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    """Check add/drop/rename/type-change against the isolated raw catalog."""
    spec = importlib.util.spec_from_file_location("load_entity", "/opt/spark-jobs/raw/load_entity.py")
    loader = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loader)
    spark = SparkSession.builder.appName("raw-schema-evolution-contract").getOrCreate()
    target = "raw.e2e_contract.schema_evolution"
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS raw.e2e_contract")
        spark.sql(f"DROP TABLE IF EXISTS {target}")
        first = spark.sql("SELECT CAST(1 AS INT) AS id, 'initial' AS label")
        spark.sql(loader.build_create_table_sql(target, loader._schema_columns(first.schema)))

        def append(frame, process_id):
            tagged = frame.withColumn("__load_dttm", F.current_timestamp()).withColumn(
                "__process_id", F.lit(process_id).cast("long")
            )
            tagged.writeTo(target).option("mergeSchema", "true").option(
                "check-ordering", "false"
            ).append()

        append(first, 1)
        append(spark.sql("SELECT CAST(2 AS INT) AS id, 'second' AS label, 'added' AS extra"), 2)
        append(spark.sql("SELECT CAST(3 AS INT) AS id, 'renamed' AS renamed_label"), 3)
        rows = spark.table(target).orderBy("id").collect()
        assert len(rows) == 3
        assert rows[0].extra is None and rows[0].renamed_label is None
        assert rows[1].extra == "added" and rows[1].label == "second"
        assert rows[2].label is None and rows[2].extra is None
        assert rows[2].renamed_label == "renamed"
        try:
            append(spark.sql("SELECT 'incompatible' AS id"), 4)
        except Exception:
            assert spark.table(target).count() == 3
        else:
            raise AssertionError("incompatible source type change was accepted")
        print("PASS raw schema add/drop/rename/type-change; FAIL=0")
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {target}")
        spark.stop()


if __name__ == "__main__":
    main()
