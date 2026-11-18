# Проверка

## Содержание

- [Команды](#команды)
- [Ожидания](#ожидания)

## Команды

```bash
python3 -m pip install -r test/requirements.txt
python3 -m ingestion.generator.generate_dags
python3 -m ingestion.generator.generate_dags --check
python3 -m pytest
./test/e2e/shell/test.sh --sources bank broker
```

## Ожидания

- Проверка генератора: `check: 0 difference(s)`.
- В группах только `load → publish_asset`, нет log_start/log_end.
- При доступном pg-meta есть START/END и FAILED для ожидаемо падающих загрузок.
- Лог задачи `load` подтверждает успешную запись события сообщением INFO со
  статусом, целевой таблицей и `process_id`.
- При ошибке записи callback пишет WARNING и не поднимает исключение.
- Полный изолированный прогон должен завершиться с FAIL=0.
