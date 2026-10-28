# datalake_stand — Инструкции проекта

## Контекст и архитектура
- Стенд КХД/DLH: Trino, Spark, Nessie, Airflow, SILO, Postgres. Источники — `sydafa`.
- Каркас: `ingestion/` (весь код процесса загрузки в git: `conf/` — YAML-манифесты, `generator/` — генератор DAG-ов, `operators.py`, `meta.py` — pg-meta, `iceberg.py`, `sources/` — диалекты СУБД и JDBC, `spark/` — Spark-приложения, в контейнерах `/opt/python/ingestion` + `PYTHONPATH=/opt/python` и `/opt/spark-jobs`), `airflow/` `trino/` (пользовательский код в git через allowlist `.gitignore`), `conf/` (как запущен стенд), `test/` (демо + e2e).
- Конфигурация источников sydafa: через `sources.json` (`conf/sydafa/sources.json` на живом стенде, `test/sydafa/sources.json` в тестах).
- Читатель источников: `sydafa`/`sydafa` (только SELECT). Админ снаружи недоступен.
- Внутренние DNS-имена СУБД (риг именует контейнеры `sydafa-src-<тип>-<source_id>`): `sydafa-src-mssql-bank:1433`, `sydafa-src-oracle-broker:1521`.
- Демо-DAG-и: `test/dags/`, подключаются `test/docker-compose.override.yaml`.
- Инспекция БД: `POST /sources/{source}/activate` → объект `connection` в ответе; внешний порт хоста задается в манифесте `sources.json` (`external_port`).
- Интеграция с sydafa: пакет `sydafa-airflow` (`SydafaActivateOperator`) для явного пробуждения СУБД перед чтением.

## Контроль веток и Pull Request (Git Workflow)
- Новые рабочие ветки — с префиксом `<github-login>/`, где логин активного аккаунта GitHub CLI определяется через `gh api user --jq .login` после проверки `gh auth status`. Не использовать фиксированный префикс `kodik/`, имя пользователя ОС или `git user.name`. Не коммитить в `main`. Обязателен Pull Request.
- **Категорический запрет на самостоятельный мерж**: Запрещено самостоятельно мержить Pull Request (через `gh pr merge`, `git merge`, `git push` в `main` или API). Агент только создаёт PR. Мерж выполняет исключительно пользователь.
- **Подробное описание PR**: при оформлении Pull Request обязательно формируй структурированное и исчерпывающее описание (Summary/Description):
  - Контекст и решаемая проблема;
  - Список конкретных изменений по компонентам;
  - Изменения в интеграциях / окружении / конфигах;
  - Свидетельства тестирования (результаты прогона тестов).

## Стандарты кода на Python
- В кодовой базе на Python категорически запрещены комментарии (строки с `#`, inline-комментарии `#`, кроме шебанга `#!/usr/bin/env python3` на первой строке исполняемых скриптов).
- Допускаются исключительно структурированные docstring (`"""..."""` или `'''...'''`) для модулей, классов и функций.

## Testing
- Для разработки, изменения и запуска тестов, а также диагностики их ошибок обязательно используй скилл `e2e-testing` (`.agents/skills/e2e-testing/SKILL.md`). Требования к покрытию, изоляции тестового контура, командам запуска и критериям успешности определены в скилле и здесь не дублируются.
