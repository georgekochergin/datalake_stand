# Маппинг типов: Oracle ↔ логический тип YAML ↔ Spark/Iceberg

| Логический тип (YAML) | Oracle DDL | Spark/Iceberg |
|---|---|---|
| `bigint` | `NUMBER(19)` | `long` |
| `string` | `VARCHAR2(400)` | `string` |
| `decimal(p,s)` | `NUMBER(p,s)` | `decimal(p,s)` |
| `date` | `DATE` | `date` |
| `timestamp` | `TIMESTAMP` | `timestamp` |

Примечания AS IS:
- Oracle `DATE` физически хранит время — для логического типа `date`
  используем именно `DATE` (без `TIMESTAMP`), сознательно принимая разницу
  в точности относительно MSSQL `DATE`.
- Отсутствие правила маппинга для нового логического типа — ошибка
  валидации при старте `datagen-api`, не "угаданный" тип.
