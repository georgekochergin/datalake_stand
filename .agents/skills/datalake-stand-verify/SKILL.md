---
name: datalake-stand-verify
description: Проверяет полную работоспособность docker-compose стенда datalake_stand: дожидается сервисов и проверяет datagen-api (:8090), Airflow (:8089), Trino HTTPS (:8443), MSSQL (:1433), Oracle (:1521) и Spark. Использовать после `docker compose up -d --build`, при сбоях доступа к сервисам или для подтверждения, что сборка работает. Не для запуска DAG-ов Airflow и не для сквозной загрузки ODS в Iceberg.
---

# Проверка сборки datalake_stand

Стенд: `/Users/admin/git/datalake_stand`.

## Шаги

1. Поднять стенд из корня репозитория: `docker compose up -d --build`.
   Если менялись пароли/DDL в `.env` или схема — сначала `docker compose down -v`
   (именованные тома `airflow_pg_data`, `pg_catalog_data`, `silo_data`,
   `datagen_state` хранят прежние креды/схему).
2. Дождаться `healthy` у `mssql-source` и `oracle-source` (30–90 c) — только
   после этого `datagen-api` выполняет provisioning БД.
3. Запустить `scripts/verify.sh` — он выполнит все проверки и выведет
   PASS/FAIL (exit code 0 при всех PASS).
4. При FAIL — открыть `references/troubleshooting.md` и искать симптом.

## Креды

| Сервис | Логин / пароль | Доступ |
| --- | --- | --- |
| Airflow UI :8089 | `admin` / `admin` | http://localhost:8089 |
| datagen-api :8090 | без авторизации | http://localhost:8090/docs |
| Trino :8443 (HTTPS) | `admin` / `admin` | https://localhost:8443 (DBeaver: `SSL=true;SSLVerification=NONE`) |
| MSSQL :1433 | `admin` / `admin` | БД `demo` |
| MSSQL bootstrap | `sa` / `AdminPassword1!` | внутренний, имя фиксировано |
| Oracle :1521 | `demo` / `admin` (SYS/SYSTEM — `admin`) | сервис `demo` |
| SILO :9001 | `admin` / `admin123` | MinIO требует пароль ≥ 8 символов |

## Проверяемые точки

- Nessie Iceberg REST `/iceberg/v1/config` → 200
- datagen-api `/docs` → 200
- Airflow login `admin`/`admin` → 201
- Trino HTTPS statement `admin`/`admin` → 200 (неверный пароль → 401)
- MSSQL БД `demo` + таблицы `customers`, `orders`, `products`
- Oracle схема `demo` + таблицы `CUSTOMERS`, `ORDERS`, `PRODUCTS`
- Spark master видит 1 живой worker (`http://localhost:8080/json/`)
