# Проверка изменения

## Содержание

- [Команды](#команды)
- [Приёмка](#приёмка)

## Команды

Из корня репозитория, Python 3.11+ с PyYAML и работающий Docker:

```bash
python3 -m unittest discover -s test/unit
python3 -m ingestion.generator.generate_dags
python3 -m ingestion.generator.generate_dags --check
./test/test.sh
```

## Приёмка

Полный прогон использует только dlh-test и удаляет его одноразовые данные. Живой стенд не менять. Ожидаются FAIL=0 и exit 0. При ошибке фиксируются фактическая причина и незавершённые сценарии, а не успешная приёмка.

Проверить исходные приложения после SIGKILL, повторы всех трёх операторов, отказ при недоступном master/невыполненном kill, другой target, независимый кластер и сохранение контракта incremental около append/finalize. Не требовать exactly-once и физического исчезновения всех executors.
