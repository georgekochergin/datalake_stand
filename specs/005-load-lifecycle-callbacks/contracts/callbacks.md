# Контракт callbacks

## Содержание

- [События](#события)
- [Развёртывание](#развёртывание)

## События

`log_task_event(context, source_id, entity_fqn, datalake_table, status, pg_conn_id="ingestion_meta")`.
Callback получает `process_id` из XCom `generate_process_id` и пишет событие через PostgresHook.
Ошибки XCom, идентификатора и записи перехватываются, предупреждение не содержит текста исключения.
Системные прерывания процесса не подавляются.

Все три оператора добавляют partial callbacks к существующим обработчикам.
on_execute → START, on_success → END, on_failure → FAILED.
on_retry не пишет FAILED и сохраняет диагностику Spark.

## Развёртывание

Удалены публичные `TableLogOperator` и `log_task_failed`. В репозитории их потребители
обновлены. DAG необходимо регенерировать одновременно с обновлением пакета:
`python3 -m ingestion.generator.generate_dags`. Старые сгенерированные DAG несовместимы
с новым экспортом. Миграция БД не нужна.
