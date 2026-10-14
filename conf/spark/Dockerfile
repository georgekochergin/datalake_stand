FROM tabulario/spark-iceberg:3.5.5_1.8.1

ADD https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.10.0.jre11/mssql-jdbc-12.10.0.jre11.jar \
    /opt/spark/jars/mssql-jdbc-12.10.0.jre11.jar
ADD https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/23.8.0.25.04/ojdbc11-23.8.0.25.04.jar \
    /opt/spark/jars/ojdbc11-23.8.0.25.04.jar

COPY conf/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
