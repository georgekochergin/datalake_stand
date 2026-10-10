FROM tabulario/spark-iceberg:3.5.5_1.8.1 AS spark-base

FROM apache/airflow:3.3.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

COPY --from=spark-base /opt/spark /opt/spark

ADD https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.10.0.jre11/mssql-jdbc-12.10.0.jre11.jar \
    /opt/spark/jars/mssql-jdbc-12.10.0.jre11.jar
ADD https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/23.8.0.25.04/ojdbc11-23.8.0.25.04.jar \
    /opt/spark/jars/ojdbc11-23.8.0.25.04.jar

COPY spark/conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

RUN chmod -R a+rX /opt/spark

USER airflow

RUN pip install --no-cache-dir --user pyspark==3.5.5

ENV SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:${PATH}"

COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY --chown=50000:0 airflow/simple_auth_manager_passwords.json /opt/airflow/simple_auth_manager_passwords.json.generated
