# Маппинг типов: MSSQL ↔ логический тип YAML ↔ Spark/Iceberg

| Логический тип (YAML) | MSSQL DDL | Spark/Iceberg |
|---|---|---|
| `bigint` | `BIGINT` | `long` |
| `string` | `NVARCHAR(400)` | `string` |
| `decimal(p,s)` | `DECIMAL(p,s)` | `decimal(p,s)` |
| `date` | `DATE` | `date` |
| `timestamp` | `DATETIME2` | `timestamp` |

Примечания AS IS:
- Имена колонок переносятся без изменений.
- `NVARCHAR` выбран по умолчанию для `string` (юникод, типично для 1С/MSSQL).
- Отсутствие правила маппинга для нового логического типа в YAML — ошибка
  валидации при старте `datagen-api` (`render_ddl.py`), а не "угаданный" тип.
