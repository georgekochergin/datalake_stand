"""build_marts_demo: учебная демонстрация двух способов трансформации
данных внутри DataLake — Trino SQL и Spark. schedule=None, запуск вручную.
Таски независимы, не обязаны давать идентичный результат (см. docs/PLAN.md).
"""
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.trino.operators.trino import TrinoOperator

with DAG(
    dag_id="build_marts_demo",
    schedule=None,
    catchup=False,
    tags=["mart", "demo"],
) as dag:
    mart_via_trino = TrinoOperator(
        task_id="mart_via_trino",
        sql="sql/build_mart_trino_demo.sql",
        trino_conn_id="trino_default",
    )

    mart_via_spark = SparkSubmitOperator(
        task_id="mart_via_spark",
        application="/opt/spark-jobs/build_mart_spark_demo.py",
        conn_id="spark_default",
        deploy_mode="cluster",
    )
