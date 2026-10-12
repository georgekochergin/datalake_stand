"""ods_load_mssql: AS IS загрузка mssql-source -> Iceberg ods.mssql_*.
Демонстрационный DAG уровня источника MSSQL.
По умолчанию выключен (is_paused_upon_creation=True).
"""
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator


def _validate_load_placeholder(**_) -> None:
    print("[validate_load] placeholder: метод валидации (см. docs/README.md)")


with DAG(
    dag_id="ods_load_mssql",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["ods", "mssql", "demo"],
) as dag:
    extract_load = SparkSubmitOperator(
        task_id="extract_load",
        application="/opt/spark-jobs/load_ods_mssql.py",
        conn_id="spark_default",
        deploy_mode="client",
    )

    validate_load = PythonOperator(
        task_id="validate_load",
        python_callable=_validate_load_placeholder,
    )

    extract_load >> validate_load
