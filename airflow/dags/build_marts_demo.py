"""build_marts_demo: учебная демонстрация двух способов трансформации
данных внутри DataLake — Trino SQL и Spark. schedule=None, запуск вручную.
Таски независимы, не обязаны давать идентичный результат (см. docs/README.md).
"""
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.trino.hooks.trino import TrinoHook

TRINO_SQL_PATH = "/opt/airflow/sql/build_mart_trino_demo.sql"


def _build_mart_via_trino() -> None:
    with open(TRINO_SQL_PATH, encoding="utf-8") as f:
        sql = f.read()
    TrinoHook(trino_conn_id="trino_default").run(sql)


with DAG(
    dag_id="build_marts_demo",
    schedule=None,
    catchup=False,
    tags=["mart", "demo"],
) as dag:
    mart_via_trino = PythonOperator(
        task_id="mart_via_trino",
        python_callable=_build_mart_via_trino,
    )

    mart_via_spark = SparkSubmitOperator(
        task_id="mart_via_spark",
        application="/opt/spark-jobs/build_mart_spark_demo.py",
        conn_id="spark_default",
        deploy_mode="client",
    )
