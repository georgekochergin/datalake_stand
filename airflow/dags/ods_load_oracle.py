"""ods_load_oracle: AS IS загрузка oracle-source -> Iceberg ods.oracle_*.
См. замечание о валидации в ods_load_mssql.py.
"""
from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator

ODS_ORACLE_DATASET = Dataset("ods://oracle")


def _validate_load_placeholder(**_) -> None:
    print("[validate_load] placeholder: метод валидации не определён (см. docs/PLAN.md)")


with DAG(
    dag_id="ods_load_oracle",
    schedule=None,
    catchup=False,
    tags=["ods", "oracle"],
) as dag:
    extract_load = SparkSubmitOperator(
        task_id="extract_load",
        application="/opt/spark-jobs/load_ods_oracle.py",
        conn_id="spark_default",
        deploy_mode="cluster",
    )

    validate_load = PythonOperator(
        task_id="validate_load",
        python_callable=_validate_load_placeholder,
        outlets=[ODS_ORACLE_DATASET],
    )

    extract_load >> validate_load
