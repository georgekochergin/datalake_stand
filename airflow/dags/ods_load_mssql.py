"""ods_load_mssql: AS IS загрузка mssql-source -> Iceberg ods.mssql_*.

task extract_load -> task validate_load (финальный, outlets=Dataset).
Метод validate_load — открытый вопрос (см. docs/README.md): простой COUNT(*)
неверен, если в источнике были DELETE между загрузками. Здесь — заглушка.
"""
from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator

ODS_MSSQL_DATASET = Dataset("ods://mssql")


def _validate_load_placeholder(**_) -> None:
    # Заглушка: полноценная методика сверки source vs ods — открытый вопрос.
    print("[validate_load] placeholder: метод валидации не определён (см. docs/README.md)")


with DAG(
    dag_id="ods_load_mssql",
    schedule=None,
    catchup=False,
    tags=["ods", "mssql"],
) as dag:
    extract_load = SparkSubmitOperator(
        task_id="extract_load",
        application="/opt/spark-jobs/load_ods_mssql.py",
        conn_id="spark_default",
        deploy_mode="cluster",
    )

    validate_load = PythonOperator(
        task_id="validate_load",
        python_callable=_validate_load_placeholder,
        outlets=[ODS_MSSQL_DATASET],
    )

    extract_load >> validate_load
