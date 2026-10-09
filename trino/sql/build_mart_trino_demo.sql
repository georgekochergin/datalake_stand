-- build_marts_demo: демонстрационная трансформация через Trino SQL (заглушка).
-- Содержание сознательно не привязано к реальной бизнес-логике (см. docs/PLAN.md)
-- — показывает механику "трансформация через Trino" в паре с
-- build_mart_spark_demo.py ("через Spark"), без требования идентичности
-- результата между движками.

CREATE SCHEMA IF NOT EXISTS iceberg.mart_trino;

CREATE TABLE IF NOT EXISTS iceberg.mart_trino.customer_totals AS
SELECT
    customer_id,
    COUNT(*)      AS orders_count,
    SUM(amount)   AS total_amount
FROM iceberg.ods.mssql_orders
GROUP BY customer_id;
