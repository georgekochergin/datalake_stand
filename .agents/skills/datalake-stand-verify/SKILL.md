---
name: datalake-stand-verify
description: Проверяет полную работоспособность docker-compose стенда datalake_stand. Быстрая проверка компонентов — scripts/verify.sh; полный сквозной тест — scripts/full-test.sh (datagen CRUD + corner-case + DAG-и ODS и витрин, в конце гасит сборку с очисткой volume-ов).
---

# Проверка сборки datalake_stand

Стенд: `/Users/admin/git/datalake_stand`.

## Шаги

### Полный сквозной тест (рекомендуется)

1. Из корня репозитория выполнить **одну** команду — `./scripts/full-test.sh`.
   Скрипт сам: чистит прежние контейнеры/volume-ы (`down -v`), поднимает стек
   (`up --build`), прогоняет весь флоу в контейнере `test-runner` и в конце
   снова гасит сборку (`down -v --remove-orphans`). Агент и человек запускают
   одну и ту же команду.
2. Читать финальную строку `PASS=n FAIL=m` (exit-code 0 = всё прошло).
3. При FAIL — открыть `references/troubleshooting.md`.

### Быстрая проверка компонентов (без мутаций и без гашения)

1. `docker compose up -d --build`.
2. `bash .agents/skills/datalake-stand-verify/scripts/verify.sh`.
3. При FAIL — `references/troubleshooting.md`.

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
- datagen-api `/sources` → `mssql,oracle`
- datagen-api `update`/`delete` (формализованный predicate) → 200 (предикат бьёт по 0 строк, данные не меняются)
- datagen-api `update` на oracle → 200 (проверка bind-параметров `:1..:n`)
- datagen-api отклоняет инъекцию через имя колонки → 422
- Airflow login `admin`/`admin` → 201
- Trino HTTPS statement `admin`/`admin` → 200 (неверный пароль → 401)
- MSSQL БД `demo` + таблицы `customers`, `orders`, `products`
- Oracle схема `demo` + таблицы `CUSTOMERS`, `ORDERS`, `PRODUCTS`
- Spark master видит 1 живой worker (`http://localhost:8080/json/`)
