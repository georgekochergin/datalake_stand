"""ods_load_db2: AS IS загрузка sydafa-src-mssql-db2 -> Iceberg ods.db2_*.
Демонстрационный DAG уровня источника MSSQL (db2).
По умолчанию выключен (is_paused_upon_creation=True).
"""
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator

from sydafa_airflow.operators import SydafaActivateOperator


def _validate_load_placeholder(**_) -> None:
    print("[validate_load] placeholder: метод валидации (см. README.md)")


with DAG(
    dag_id="ods_load_db2",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["ods", "mssql", "db2", "demo"],
) as dag:
    activate_source = SydafaActivateOperator(
        task_id="activate_source",
        conn_id="sydafa_default",
        source="db2",
        ttl_minutes=15,
    )

    extract_load = SparkSubmitOperator(
        task_id="extract_load",
        application="/opt/spark-jobs/load_ods_db2.py",
        conn_id="spark_default",
        deploy_mode="client",
    )

    validate_load = PythonOperator(
        task_id="validate_load",
        python_callable=_validate_load_placeholder,
    )

    activate_source >> extract_load >> validate_load
