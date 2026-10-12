"""Автоматически сгенерированный демо-DAG ods_load_oracle_demo.
Сгенерирован сервисом sydafa на основе метаданных схемы источника 'oracle', схема 'demo'.
По умолчанию DAG выключен (is_paused_upon_creation=True).
"""
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator


def _validate_load_placeholder(**_) -> None:
    print("[validate_load] placeholder: метод валидации (см. docs/README.md)")


with DAG(
    dag_id="ods_load_oracle_demo",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["ods", "oracle", "demo", "demo"],
) as dag:
    extract_load = SparkSubmitOperator(
        task_id="extract_load",
        application="/opt/spark-jobs/load_ods_oracle.py",
        conn_id="spark_default",
        deploy_mode="client",
    )

    validate_load = PythonOperator(
        task_id="validate_load",
        python_callable=_validate_load_placeholder,
    )

    extract_load >> validate_load
