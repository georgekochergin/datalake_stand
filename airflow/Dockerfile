FROM apache/airflow:3.3.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# spark-submit + Spark 3.5.5 client (та же версия, что на spark-master/worker,
# см. spark/Dockerfile) — нужен локально для SparkSubmitOperator.
# ⚠ проверить на этапе реализации: provider apache-spark==6.3.2 требует
# pyspark-client>=4.0.0 (Spark Connect) — подтвердить, что SparkSubmitOperator
# всё ещё работает через классический spark-submit (см. docs/README.md).
RUN pip install --no-cache-dir --user pyspark==3.5.5
ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Простой менеджер аутентификации SimpleAuthManager читает пароли из файла
# /opt/airflow/simple_auth_manager_passwords.json.generated. Кладём туда admin/admin
# заранее, чтобы не полагаться на сломанную в Airflow 3.3.2 команду
# `airflow users create` (баг в apache-airflow-providers-fab 3.9.0).
# --chown=50000:0 — пользователь airflow (uid 50000) должен открывать файл на запись.
COPY --chown=50000:0 simple_auth_manager_passwords.json /opt/airflow/simple_auth_manager_passwords.json.generated
