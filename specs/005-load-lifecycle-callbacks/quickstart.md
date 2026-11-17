# Проверка

## Содержание

- [Команды](#команды)
- [Ожидания](#ожидания)

## Команды

```bash
python3 -m ingestion.generator.generate_dags
python3 -m ingestion.generator.generate_dags --check
python3 -m unittest discover -s test/unit
./test/test.sh
```

На хосте нужен PyYAML. При его отсутствии можно использовать временное окружение
`uv run --with pyyaml`, не меняя зависимости проекта.

## Ожидания

- Проверка генератора: `check: 0 difference(s)`.
- В группах только `load → publish_asset`, нет log_start/log_end.
- При доступном pg-meta есть START/END и FAILED для ожидаемо падающих загрузок.
- При ошибке записи callback пишет WARNING и не поднимает исключение.
- Полный изолированный прогон должен завершиться с FAIL=0.
