# Бэклог

## Оглавление

- [Глоссарий](glossary.md)
- [Приоритет: важно](#приоритет-важно)
  - [Определить и проверить минимальные поддерживаемые версии компонентов](#определить-и-проверить-минимальные-поддерживаемые-версии-компонентов)
  - [Разделить реестр источников на YAML-файлы и настроить JDBC-параллелизм по источникам](#разделить-реестр-источников-на-yaml-файлы-и-настроить-jdbc-параллелизм-по-источникам)
  - [Изолировать Spark drivers через DockerOperator](#изолировать-spark-drivers-через-dockeroperator)
  - [Рефакторинг и переорганизация кода: структура модулей](#рефакторинг-и-переорганизация-кода-структура-модулей)
  - [Обработка подвисших процессов Spark при retry задач](#обработка-подвисших-процессов-spark-при-retry-задач)
  - [Шаблонизация кода в generator/generate_dags.py](#шаблонизация-кода-в-generatorgenerate_dagspy)
  - [Управление ресурсами Spark на уровне отдельной джобы](#управление-ресурсами-spark-на-уровне-отдельной-джобы)
- [Приоритет: можно отложить](#приоритет-можно-отложить)
  - [Автоматически формировать теги DAG и исключать дубликаты](#автоматически-формировать-теги-dag-и-исключать-дубликаты)
  - [Разделить пулы Airflow для загрузок и служебных задач](#разделить-пулы-airflow-для-загрузок-и-служебных-задач)
  - [Убрать пароль JDBC-источника из аргументов spark-submit](#убрать-пароль-jdbc-источника-из-аргументов-spark-submit)
  - [Выбирать JDBC-колонку по метрикам с управляемым повторным анализом](#выбирать-jdbc-колонку-по-метрикам-с-управляемым-повторным-анализом)

Бэклог изменяется только с разрешения пользователя.
Приоритет задачи задаётся разделом: `важно` или `можно отложить`.
Ниже сохранены согласованные задачи и их приоритеты. Описанные решения
ещё не означают, что возможности реализованы.

## Приоритет: важно

### Определить и проверить минимальные поддерживаемые версии компонентов

#### Контекст

Текущая конфигурация задаёт конкретные версии, но не подтверждённую нижнюю границу совместимости проекта:

- `conf/airflow/Dockerfile`: Airflow `3.3.2-python3.11`, PySpark `3.5.5`, Java 17; Spark-дистрибутив копируется из `tabulario/spark-iceberg:3.5.5_1.8.1`.
- `conf/airflow/requirements.txt`: Spark provider `6.3.2`, Trino provider `6.7.0`; `sydafa-airflow` устанавливается из Git без закреплённой ревизии.
- `conf/spark/Dockerfile`: связка Spark `3.5.5` / Iceberg `1.8.1`, MSSQL JDBC `12.10.0.jre11` и Oracle JDBC `23.8.0.25.04`.
- `test/Dockerfile`: отдельный test runner на Python `3.12`.
  Зависимости генератора — в `ingestion/generator/requirements.txt`.
  Версии по компонентам — в [README](../README.md#языки-и-библиотеки).
- Код операторов и генератора использует `airflow.sdk`, `Asset`, а Spark-код — типы `timestamp_ltz`/`timestamp_ntz` и Iceberg DDL. Такие API и зависимости ограничивают возможное понижение версий.

#### Проблема

Версия используемого образа не определяет нижнюю границу поддержки.
Пока неизвестно, какие более старые сочетания компонентов работают
без изменения прикладного кода.

Минимумы нужно проверять совместно:

- Airflow зависит от providers и Python.
- Spark зависит от PySpark, Java, Iceberg и JDBC-драйверов.
- Успешный импорт и старт контейнеров не подтверждают загрузки,
  точность времени, снимки и восстановление после сбоев.

Вывод основан на статическом анализе конфигурации и используемых API. Минимальные версии и успешность текущего стека в рамках оформления задачи не проверялись.

#### Предпочтительный путь решения

Установить воспроизводимую минимально поддерживаемую комбинацию компонентов для текущих возможностей проекта, проверяя понижение версий в изолированном тестовом контуре без рефакторинга прикладного кода.

1. Определить область поддержки: Python отдельно для генератора, Airflow, Spark [driver](glossary.md#term-driver)/[executors](glossary.md#term-executor) и test runner; Airflow и providers; Spark/PySpark, Java, Iceberg и JDBC-драйверы; участвующие в сценариях Trino, Nessie, SILO/S3, PostgreSQL, sydafa и версии СУБД-источников. Для Docker/Compose, ОС/архитектуры и ресурсов среды явно указать проверенные требования. Не обещать поддержку компонентов и платформ, которые не проверялись.
2. Зафиксировать точную исходную комбинацию из образов и реально установленных зависимостей, включая транзитивные версии, ревизию sydafa/sydafa-airflow и Python/Java внутри образов. Сначала получить успешный контрольный прогон; не принимать текущие Dockerfile за свидетельство успешного запуска.
3. По официальным требованиям зависимостей и используемым API определить кандидатов нижней границы. Отличать теоретическое ограничение от проверенного результата; не объявлять точные минимумы только по синтаксису Python или номеру образа. Airflow 2 не включать в обещанную поддержку текущего SDK/Asset-кода без доказанной совместимости и не добавлять compatibility layers ради его поддержки.
4. Понижать компоненты контролируемыми шагами, сохраняя согласованные группы зависимостей: Airflow/providers/Python и Spark/PySpark/Java/Iceberg/JDBC. Каждый кандидат проверять отдельно, затем обязательно проверять итоговую нижнюю комбинацию совместно. Для отклонённого ближайшего более низкого кандидата сохранять конкретное ограничение API/зависимости либо результат отказа; полный перебор всех patch-версий и декартова матрица всех сочетаний не требуются.
5. Разрешены изменения версий, constraints, образов и параметров тестовой сборки, необходимые для воспроизведения. Не менять семантику загрузчиков, не ослаблять проверки и не переписывать код под старые API. Если кандидат требует рефакторинга, он находится ниже границы этой задачи. Недоступный образ или непроведённая проверка означают «не проверено», а не доказанную несовместимость.
6. Сохранить штатную конфигурацию стенда как рекомендуемую сборку; минимальный профиль проверять отдельно без воздействия на основной контур и его данные. Зафиксировать воспроизводимые версии/ревизии зависимостей обоих профилей; не полагаться на плавающие latest или Git HEAD в свидетельствах совместимости.
7. В README, раздел требований, документировать рекомендуемую и минимально проверенную комбинации, ограничения по ролям Python и допустимым сочетаниям. Архитектурные ограничения API и типов описать в `docs/architecture.md`. Не трактовать успешность нижней и текущей сборок как гарантию для всех промежуточных или будущих версий: заявлять только проверенные профили и явно обоснованные ограничения.

Проверки и критерии готовности:

- Для рекомендуемого и минимального профилей сохранены точный состав окружения, команды воспроизведения и результаты проверок; минимальный профиль воспроизводится без изменения прикладного кода.
- Пройдены релевантные [unit-проверки](glossary.md#term-unit), генерация/`--check`, импорт DAG, сборка и запуск сервисов, полный изолированный `./test/test.sh` — `FAIL=0`, exit 0.
- Покрыты MSSQL/Oracle, [full](glossary.md#term-full)/[incremental](glossary.md#term-increment), актуальные снимки, schema evolution, JDBC-партиционирование, точность временных позиций, пустые окна, [watermark](glossary.md#term-watermark) и [retry](glossary.md#term-retry)/recovery. Для [driver](glossary.md#term-driver)/[executors](glossary.md#term-executor) проверены фактические версии; ограничение Python UDF/RDD при несовпадающих minor-версиях Python указано явно, а не выдано за поддержку.
- Для сервисов вне полного [e2e](glossary.md#term-e2e)-покрытия указаны отдельные smoke/integration-проверки либо статус «не проверено». Ограничения безопасности/EOL отмечены отдельно: техническая совместимость не делает устаревшую версию рекомендуемой.
- Обоснована нижняя поддерживаемая комбинация, отличены фактическая несовместимость и выбранная граница поддержки; документация не содержит неподтверждённых обещаний диапазонов версий. Изменения тестов и запуск проверок при реализации выполняются по скиллу `e2e-testing`; здесь добавляется только задача бэклога.

### Разделить реестр источников на YAML-файлы и настроить JDBC-параллелизм по источникам

#### Контекст

Реестр `ingestion/conf/sources.yaml` содержит настройки всех источников:
ключ, `type` и `airflow_conn_id`. Его читает
`ingestion/generator/manifest.py::load_sources`.

Манифесты [пайплайнов](glossary.md#term-pipeline) находятся в `ingestion/conf/pipelines/`.
Они ссылаются на источник через `source_id`.
Один источник может использоваться несколькими [пайплайнами](glossary.md#term-pipeline).

Число JDBC-партиций задано глобально: `ingestion/conf/settings.yaml`
содержит `spark.jdbc.num_partitions: 4`.
Генератор передаёт его в обычный DAG и DAG снимка, затем оператор —
в `--num-jdbc-partitions` загрузчика.
Переключатель `jdbc_partitioning` задаётся отдельно для [сущности](glossary.md#term-entity) и снимка.

#### Проблема

Параллельное добавление разных источников требует изменения одного общего YAML, повышая риск merge-конфликтов. Единое число JDBC-партиций также не позволяет настроить нагрузку чтения с учётом конкретного источника.

Пример: две ветки добавляют разные источники в конец `sources.yaml`. Изменения логически независимы, но затрагивают один участок файла. Раздельные файлы устраняют эту общую точку редактирования, хотя не гарантируют отсутствие всех merge-конфликтов.

Вывод основан на статическом анализе загрузчика конфигурации и генератора; конфликт слияния и [runtime](glossary.md#term-runtime)-параллелизм в рамках оформления задачи не воспроизводились.

#### Предпочтительный путь решения

Заменить общий реестр каталогом `ingestion/conf/sources/`: один источник — один `<source_id>.yaml`. [Pipelines](glossary.md#term-pipeline) не объединять с источниками и не ограничивать их число на источник.

- Выводить `source_id` из имени файла без расширения, проверять по существующим правилам namespace. Не дублировать идентификатор ключом внутри YAML; содержимое — настройки одного источника.
- Использовать плоский каталог с `.yaml`, детерминированный порядок загрузки и строгую проверку полей. Сообщать путь файла при ошибке; отклонять пустой реестр, недопустимые имена, неизвестные поля и повторные идентификаторы, не перезаписывая источник молча.
- Добавить в конфигурацию источника необязательный `spark.jdbc.num_partitions`: положительное целое, без bool; при отсутствии применять 4. Не добавлять переопределение этого числа на уровне [pipeline](glossary.md#term-pipeline) или [сущности](glossary.md#term-entity).

Пример `ingestion/conf/sources/bank.yaml`:

```yaml
type: rdbms
airflow_conn_id: bank_source
spark:
  jdbc:
    num_partitions: 4
```

- Передавать значение из `SourceConfig` в обычные и [snapshot](glossary.md#term-snapshot) DAG, затем в соответствующие операторы и Spark CLI. Сохранить `jdbc_partitioning` на уровне [сущности](glossary.md#term-entity)/[snapshot](glossary.md#term-snapshot): при отключении чтение остаётся непартиционированным независимо от настройки источника. Фактическое число партиций может быть меньше запрошенного по действующим правилам reader.
- Число партиций ограничивает параллелизм одной загрузки, а не суммарное число JDBC-соединений всех параллельных загрузок источника. Не вводить в этой задаче общий connection budget, [pools](glossary.md#term-pool) или resource limits Spark.
- Мигрировать `bank` и `broker`, переключить `--sources` генератора с файла на каталог, обновить потребителей и документацию. Удалить глобальный `spark.jdbc.num_partitions`; поскольку сейчас это единственное содержимое settings, убрать `settings.yaml` и связанные `--settings`/`load_settings`, не оставляя пустую конфигурацию или совместимый двойной источник значения. Миграцию CLI описать явно.
- Сохранить ссылки [pipeline](glossary.md#term-pipeline) по `source_id`, DAG/task IDs, targets и существующие проверки коллизий DAG/target во всём наборе [pipelines](glossary.md#term-pipeline). Добавление независимого источника не должно требовать редактирования центрального списка или общего файла настроек.
- Согласовать с задачей [«Управление ресурсами Spark на уровне отдельной джобы»](#управление-ресурсами-spark-на-уровне-отдельной-джобы): её описание текущих глобальных JDBC-настроек относится к состоянию до этой миграции; ресурсы приложения остаются отдельными параметрами [сущности](glossary.md#term-entity), а JDBC-параллелизм после миграции задаётся источником.

Проверки и критерии готовности:

- [Unit](glossary.md#term-unit): идентификатор из имени файла, строгая схема, ошибки с путём, дефолт 4, разные значения для двух источников, отклонение 0/отрицательных/дробных/bool значений и неизвестных полей; отсутствие зависимости от порядка файлов.
- Генерация обычных и [snapshot](glossary.md#term-snapshot) DAG передаёт значение соответствующего источника; отключение `jdbc_partitioning` сохраняется. При миграции существующих источников с дефолтом 4 семантика и идентификаторы DAG не меняются; генерация/`--check` успешны.
- Независимые источники добавляются отдельными файлами без изменений центрального реестра; ссылки на неизвестный источник и коллизии DAG/target по-прежнему отклоняются.
- [E2E](glossary.md#term-e2e): загрузки MSSQL/Oracle и актуальные снимки проходят в изолированном контуре; параметры чтения соответствуют источнику. Полный `./test/test.sh` — `FAIL=0`, exit 0.
- README и архитектурная документация обновлены по своим зонам ответственности, граф актуализирован при реализации. Реализация выполняется отдельно по спек-циклу; здесь добавляется только задача бэклога.

### Изолировать Spark drivers через DockerOperator

#### Контекст

Spark [driver](glossary.md#term-driver) — управляющий процесс приложения: выполняет основной код, строит план вычислений, распределяет задачи между [executors](glossary.md#term-executor) и получает результаты. [Executors](glossary.md#term-executor) выполняют распределённую обработку данных на Spark workers.

`deploy_mode` определяет место запуска [driver](glossary.md#term-driver), а не [executors](glossary.md#term-executor):

- `client`: [driver](glossary.md#term-driver) работает там, где запущен `spark-submit`.
- `cluster`: [driver](glossary.md#term-driver) размещается менеджером вычислительного кластера.

Текущий запуск:

- Spark Standalone 3.5.5 и Airflow LocalExecutor.
- `IngestAppendOperator` наследует `SparkSubmitOperator`, режим — `client`.
- `spark-submit` и [driver](glossary.md#term-driver) работают в `airflow-scheduler`.
- [Executors](glossary.md#term-executor) работают на `spark-worker`.

Исходные файлы:

- Загрузчики: `ingestion/spark/raw/rdbms/incremental/load_entity.py`
  и `ingestion/spark/raw/rdbms/full/load_entity.py`.
- Оператор: `ingestion/operators.py`.
- Окружение: `docker-compose.yaml` и `conf/airflow/Dockerfile`.

Spark Standalone 3.5.5 не поддерживает `cluster` deploy mode для Python-приложений. Поэтому одной сменой `deploy_mode` изолировать текущий PySpark [driver](glossary.md#term-driver) нельзя. Production не требует именно cluster mode: важны изоляция ресурсов и управляемый жизненный цикл [driver](glossary.md#term-driver).

#### Проблема

[Driver](glossary.md#term-driver) потребляет CPU и память контейнера Airflow. Несколько параллельных загрузок создают несколько [drivers](glossary.md#term-driver)/JVM рядом со scheduler и конкурируют с оркестратором за ресурсы. При остановке этого контейнера текущая загрузка теряет [driver](glossary.md#term-driver) и не может нормально завершиться.

Airflow [pools](glossary.md#term-pool) ограничивают допуск и вес одновременно работающих загрузок, но не обеспечивают отдельные CPU/memory limits для каждого [driver](glossary.md#term-driver). Образ Airflow также содержит Spark, Java и JDBC-зависимости.

Пример: две одновременно работающие загрузки запускают два [driver](glossary.md#term-driver)-процесса в `airflow-scheduler`, хотя [executors](glossary.md#term-executor) выполняются на отдельном worker.

Кейс воспроизведения (не проверен в рамках оформления бэклога): запустить две параллельные загрузки и проверить размещение процессов и потребление ресурсов. Ожидаемое целевое поведение — отдельный контейнер с CPU/memory limits для каждого [driver](glossary.md#term-driver); текущее поведение по описанию выше — оба [driver](glossary.md#term-driver) работают в контейнере scheduler и делят его ресурсы.

#### Предпочтительный путь решения

Перевести предметный `IngestAppendOperator` на `DockerOperator`: одна задача загрузки — отдельный контейнер со `spark-submit` и [driver](glossary.md#term-driver). Сохранить Spark Standalone и `deploy_mode=client`.

```text
Airflow: IngestAppendOperator(DockerOperator)
    → контейнер загрузки: spark-submit + driver
        → Spark Standalone: executors на worker
```

Результат перехода:

- Окружение и ресурсы [driver](glossary.md#term-driver) отделены от scheduler.
- Для каждой загрузки можно задать лимиты CPU и памяти.
- Остановка scheduler не завершает [driver](glossary.md#term-driver) непосредственно.

Ограничения:

- Сохранение контейнера, мониторинг и восстановление управления требуют реализации.
- Сам переход не гарантирует продолжение загрузки после сбоя Airflow.
- Автоматическое восстановление и отсутствие повторных записей не обеспечиваются одной контейнеризацией.

##### Образ и зависимости

- Адаптировать существующую Spark-сборку без необоснованного дублирования. Включить загрузчик, Spark/Python, Iceberg и MSSQL/Oracle JDBC drivers в образ, без host bind mounts кода для каждого запуска.
- Проверить совместимость Java/Python и библиотек [driver](glossary.md#term-driver)/[executors](glossary.md#term-executor); для Python workers требуется совпадение minor-версий Python.
- Передавать конфигурацию каталога и credentials при запуске; не запекать secrets в образ.
- Определить воспроизводимые build/tag и сборку образа до запуска DAG основного и [e2e](glossary.md#term-e2e)-контуров.
- Добавить совместимый Docker provider. Убирать Spark/Java/SparkSubmit provider из Airflow image только после проверки остальных потребителей, включая пользовательские DAG.

##### Контракт оператора

- Сохранить [runtime](glossary.md#term-runtime)-разрешение Airflow Connections/source.type, получение process_id и [watermark](glossary.md#term-watermark), [pool](glossary.md#term-pool)/pool_slots.
- [Retry](glossary.md#term-retry) запускает новый контейнер, но сохраняет process_id своего DAG run.
- Собирать команду без небезопасной shell interpolation; исключить пароли из командной строки, logs и rendered fields. Environment не является защищённым secret store: значения доступны привилегированному Docker API.
- Ожидать завершения контейнера, передавать stdout/stderr в task logs, ненулевой exit code трактовать как failure.

##### Сеть и ресурсы

- Подключать контейнер к сети соответствующего compose-проекта; не смешивать основной и тестовый контуры.
- Обеспечить доступ к Spark master, источникам, Nessie/SILO и обратное подключение [executors](glossary.md#term-executor) к [driver](glossary.md#term-driver). Настроить уникальные имена/адреса попыток, `spark.driver.host`, bind address и отсутствие конфликтов портов параллельных [drivers](glossary.md#term-driver).
- Определить конфигурацию образа, CPU/memory limits и необходимых Spark resources; точные ключи — по версии DockerOperator. Согласовать место хранения с задачей [«Разделить реестр источников на YAML-файлы и настроить JDBC-параллелизм по источникам»](#разделить-реестр-источников-на-yaml-файлы-и-настроить-jdbc-параллелизм-по-источникам): существующий `ingestion/conf/settings.yaml` содержит только глобальный JDBC-параллелизм и в рамках той миграции удаляется. Не расширять его настройками DockerOperator и не восстанавливать после удаления; отдельный контракт конфигурации запуска определить при проектировании DockerOperator, независимо от порядка реализации задач. Ресурсы Spark на уровне [сущности](glossary.md#term-entity) согласовать с задачей [«Управление ресурсами Spark на уровне отдельной джобы»](#управление-ресурсами-spark-на-уровне-отдельной-джобы), не создавая второй источник тех же значений.
- Согласовать JVM memory с лимитом контейнера, оставив запас для Python, JVM overhead и native memory.
- Разделять Airflow [slots](glossary.md#term-slot) (admission control), Docker limits (ресурсы [driver](glossary.md#term-driver)) и Spark/JDBC parallelism (распределённое выполнение).

##### Жизненный цикл

- Определить удаление контейнеров после success/failure и доступность логов после удаления.
- При отмене задачи останавливать её контейнер и проверять освобождение [executors](glossary.md#term-executor).
- Помечать контейнеры контуром, DAG run, task и попыткой; предусмотреть scoped cleanup orphan-контейнеров после аварии scheduler.
- Проверить temporary mounts DockerOperator: пути внутри Airflow не обязательно доступны Docker daemon host.

##### Риски и границы решения

- Docker API/socket даёт широкие права на Docker-хост. Read-only mount socket не ограничивает операции API. Доступ предоставлять только компоненту, исполняющему задачи; не исправлять права глобальным небезопасным chmod и не требовать privileged-контейнеров.
- Контейнеризация [driver](glossary.md#term-driver) изолирует ресурсы, но сама по себе не создаёт строгую границу безопасности, не устраняет сетевые зависимости и добавляет управление образами, orphan cleanup и обработку cancellation.
- Production-топология и возможная смена платформы выполнения — отдельное решение, не обязательная часть этой задачи. Здесь выбран DockerOperator со Spark Standalone в client mode; выбор иной платформы не входит в объём работ.

##### Проверки и критерии готовности

- [Unit](glossary.md#term-unit): настройки команды, credentials transport, Connection resolution, [pool](glossary.md#term-pool)/[slots](glossary.md#term-slot), уникальные labels/сеть, resource limits, exit code и cancellation; отсутствие паролей в команде/logs/rendered fields.
- [E2E](glossary.md#term-e2e): четыре демонстрационных DAG работают из отдельных [driver](glossary.md#term-driver)-контейнеров; проверены [executor](glossary.md#term-executor) → [driver](glossary.md#term-driver) и параллельные [drivers](glossary.md#term-driver); [driver](glossary.md#term-driver) не выполняется в scheduler.
- Проверены failure/[retry](glossary.md#term-retry) со сквозным process_id, cleanup success/failure/cancel и отсутствие воздействия на чужой контур.
- Сохранены [snapshot](glossary.md#term-snapshot)/[incremental](glossary.md#term-increment)/schema-evolution проверки. Обновлены README.md и описание PR при наличии доступа.
- [Unit](glossary.md#term-unit), генерация/`--check` и полный `./test/test.sh` проходят; [e2e](glossary.md#term-e2e) — `FAIL=0`, exit 0. Недоступные fault/cancellation проверки указывать явно.

Задача не является предварительным условием работы над [инкрементом](glossary.md#term-increment).

### Рефакторинг и переорганизация кода: структура модулей

#### Контекст

Модули `ingestion/` смешивают общую инфраструктуру и логику отдельных режимов загрузки. Статический анализ показал следующие границы:

- `ingestion/operators.py::IngestAppendOperator` выбирает Spark application по наличию `incremental_config`. Его `execute` для обоих режимов получает pg-meta Connection, передаёт `PG*` и идентификаторы DAG run; только [incremental](glossary.md#term-increment) использует реестр [batch](glossary.md#term-batch). `IngestActSnapOperator` уже отделён, но повторяет получение JDBC Connection, process_id и сборку общих аргументов.
- Spark entry points уже разделены: `ingestion/spark/raw/rdbms/incremental/load_entity.py` планирует фиксированное окно и управляет [batch](glossary.md#term-batch)/[watermark](glossary.md#term-watermark); `full/load_entity.py` читает весь источник и добавляет [полный срез](glossary.md#term-full) без реестра [инкремента](glossary.md#term-increment). Полная загрузка здесь — append, не overwrite; повтор может добавить срез повторно ([at-least-once](glossary.md#term-atleast)).
- `ingestion/spark/raw/rdbms/reader.py::read_extract_frame` и `build_extract_query` определяют [incremental](glossary.md#term-increment) через `getattr(args, "incremental_column", None)`: добавляют границы окна и `__extract_position`. Общими остаются JDBC-доступ, compatibility projection, выбор партиционирования, MIN/MAX и кэширование.
- `ingestion/spark/raw/rdbms/raw_append.py::append_raw` аналогично совмещает общий append с вычислением максимальной позиции и удалением `__extract_position` только для [incremental](glossary.md#term-increment).
- `ingestion/generator/manifest.py::EntityConfig` содержит optional [incremental](glossary.md#term-increment)-поля и `act_snap` даже для [full](glossary.md#term-full); `_parse_entity` заполняет их условно. Генератор DAG использует и `incremental_config`, и `incremental_column` как признаки режима.
- `ingestion/meta.py` содержит как общее логирование DAG/task, так и реестр [инкремента](glossary.md#term-increment). `ingestion/sources/rdbms/contracts.py` объединяет контракты подключения, парсер таблицы и базовый `RdbmsDialect`.

#### Проблема

Отсутствие явной структуры затрудняет:

- Ориентировку в коде при введении новых разработчиков.
- Выделение ответственности каждого компонента.
- Поиск и переиспользование функций в других модулях.
- Локализацию изменений при внесении улучшений.

Пример: изменение [incremental](glossary.md#term-increment)-статистики затрагивает общий `append_raw`, хотя [full](glossary.md#term-full) не использует позицию; [full](glossary.md#term-full)-оператор получает реквизиты реестра [инкремента](glossary.md#term-increment), хотя его Spark-приложение не обращается к этому реестру. Отсутствие [incremental](glossary.md#term-increment)-поля неявно заменяет явный контракт полного чтения.

Решение по результатам анализа: разделение флоу оправдано, но дублировать JDBC, диалекты и Iceberg-запись не нужно. [Runtime-проверки](glossary.md#term-runtime) в рамках оформления задачи не выполнялись.

#### Предпочтительный путь решения

Разделить оркестрацию и режимные контракты, сохранив общие технические операции без флагов [full](glossary.md#term-full)/[incremental](glossary.md#term-increment) внутри них.

1. **Операторы**: заменить `IngestAppendOperator` на `IngestIncrementalOperator` и `IngestFullOperator`, каждый с фиксированным Spark entry point. Первый требует [incremental](glossary.md#term-increment)-конфигурацию и передаёт окружение реестра; второй не принимает [incremental](glossary.md#term-increment)-конфигурацию и не получает `PG*` для Spark [driver](glossary.md#term-driver). Сохранить pg-meta для общего логирования [full](glossary.md#term-full): process_id, START/END/FAILED и DAG callbacks нужны обоим режимам. Общую подготовку JDBC-подключения, process_id, аргументов, [pool](glossary.md#term-pool)/[slots](glossary.md#term-slot) и failure callback вынести в небольшой общий слой, используемый также `IngestActSnapOperator`; не создавать иерархию стратегий и плагинов.
2. **Конфигурация и генератор**: выделить `FullEntityConfig` и `IncrementalEntityConfig` с общими полями [сущности](glossary.md#term-entity) и загрузки. Только [incremental](glossary.md#term-increment) содержит контракт позиции, [lookback](glossary.md#term-lookback) и `act_snap`; `incremental_column` получать из одного контракта, а не хранить независимым признаком режима. Сохранить внешний YAML: наличие секции `incremental` определяет тип один раз при разборе. Генератор выбирает конкретный оператор по типу конфигурации; единственная развилка на границе допустима. Сохранить DAG/task IDs, зависимости, расписания, Assets и отдельную генерацию [snapshot](glossary.md#term-snapshot) DAG только для [incremental](glossary.md#term-increment).
3. **Чтение Spark**: оставить существующие отдельные entry points и вынести подготовку окна, предикатов и `__extract_position` в [incremental](glossary.md#term-increment)-область. [Full](glossary.md#term-full) и [snapshot](glossary.md#term-snapshot) используют явное чтение полного источника. Общий reader получает готовые проекции/предикаты и выполняет JDBC-чтение и партиционирование, не исследуя `args.incremental_column` и не принимая фиктивные `batch=None, kind=None` как переключатель режима. Не дублировать compatibility projection, bounds и partitioning.
4. **Статистика и запись**: в [incremental](glossary.md#term-increment)-флоу вычислять count/max, канонизировать позицию и удалять техническую колонку; в [full](glossary.md#term-full)-флоу вычислять только count. Общий writer получает очищенный DataFrame и число строк, отвечает за валидацию схемы, создание таблицы и append с техническими метаданными. Сохранить один проход агрегации count/max для [incremental](glossary.md#term-increment), текущую материализацию/освобождение кэша и поведение пустых загрузок. Не вводить повторные count или отдельное чтение источника ради разделения кода. [Snapshot](glossary.md#term-snapshot) [RTAS](glossary.md#term-rtas) не объединять с append-writer.
5. **Метаданные и модули**: отделить реестр [incremental](glossary.md#term-increment) [batch](glossary.md#term-batch)/state от общего логирования в pg-meta, оставив единый helper транзакций и порядок блокировок. Разместить общие helpers/callbacks и режимные операторы в отдельных модулях; сохранить `ingestion.operators` как публичную точку импорта новых операторов и общих callbacks. Не оставлять универсальный `IngestAppendOperator`-диспетчер: мигрировать потребителей репозитория и документировать замену для внешних DAG, не предполагая, что их нет.
6. **Диалекты**: вынести `RdbmsDialect` с его методами из `contracts.py` в `ingestion/sources/rdbms/dialect.py`; контракты подключения, парсинг и type helpers оставить на соответствующем уровне без циклических импортов. JDBC metadata, quoting и правила типов общие для режимов — не создавать копии диалектов для [full](glossary.md#term-full)/[incremental](glossary.md#term-increment). Сохранить существующие публичные импорты диалектов через re-export там, где это требуется.

##### Границы рефакторинга

- Сохранить форматы YAML и данные pg-meta.
- Сохранить [at-least-once](glossary.md#term-atleast) append, [watermark](glossary.md#term-watermark), [lookback](glossary.md#term-lookback), повтор фиксированного окна
  и восстановление снимков.
- Не добавлять режимы или гарантию [exactly-once](glossary.md#term-exactly) — однократной записи без дублей.
- Один [пайплайн](glossary.md#term-pipeline) по-прежнему может содержать полные и инкрементальные загрузки.
  Отдельные DAG-и по режимам не нужны.
- Переход на DockerOperator выполняется отдельно.
  Новые операторы должны сохранить своё назначение после такого перехода.
- SQL и DAG-шаблоны выносятся в задаче
  [«Шаблонизация кода в generator/generate_dags.py»](#шаблонизация-кода-в-generatorgenerate_dagspy).

#### Проверки и критерии готовности

- Все потребители `IngestAppendOperator` в репозитории переведены на конкретные операторы; замена публичного API и правила миграции документированы. Остальные публичные импорты сохранены.
- Все модули импортируют друг друга корректно; нет циклических зависимостей.
- [Unit-проверки](glossary.md#term-unit) отражают новые контракты: [full](glossary.md#term-full) не принимает [incremental](glossary.md#term-increment)-поля и не получает реестр/`PG*` в [driver](glossary.md#term-driver); [incremental](glossary.md#term-increment) требует конфигурацию и получает необходимые параметры. Общее логирование [full](glossary.md#term-full) сохранено.
- Смешанный [pipeline](glossary.md#term-pipeline) генерирует оба конкретных оператора с прежними IDs и зависимостями; [snapshot](glossary.md#term-snapshot) DAG создаётся только для [incremental](glossary.md#term-increment). Внешние YAML не требуют миграции.
- Общий reader/writer не определяет режим через optional [incremental](glossary.md#term-increment)-поля. [Full](glossary.md#term-full) читает и добавляет [полный срез](glossary.md#term-full), [incremental](glossary.md#term-increment) — фиксированное окно; сохранены точность позиции, NULL-проверки, [CAS revision](glossary.md#term-cas), пустые окна, [retry](glossary.md#term-retry) и [snapshot](glossary.md#term-snapshot) recovery без лишних чтений/агрегаций.
- Генерация DAG-ов (`python3 -m ingestion.generator.generate_dags --check`) успешна.
- Полный набор [e2e-тестов](glossary.md#term-e2e) (`./test/test.sh`) проходит.
- Граф зависимостей graphify перестроен для отражения новой структуры.
- `docs/architecture.md` и инструкция миграции обновлены вместе с реализацией. Реализация выполняется отдельно по спек-циклу; здесь изменяется только бэклог.

### Обработка подвисших процессов Spark при retry задач

#### Контекст

Airflow различает жизненные циклы отказа задачи:

- `on_failure_callback`: вызывается при финальном отказе (исчерпаны все [retry](glossary.md#term-retry)).
- `on_retry_callback`: вызывается перед каждым повтором задачи.

Текущий `IngestAppendOperator` (унаследован от `SparkSubmitOperator`) использует `on_failure_callback` для логирования конечного отказа в meta-БД. Однако на [retry](glossary.md#term-retry) процесс Spark ([driver](glossary.md#term-driver) и распределённые [executors](glossary.md#term-executor)) может оставаться запущенным и не контролируемым, если потребляет ресурсы или блокирует порты.

#### Проблема

При [retry](glossary.md#term-retry) задачи:

1. Spark [driver](glossary.md#term-driver)/[executors](glossary.md#term-executor) из предыдущей попытки могут остаться запущенными (особенно при сбое scheduler или сетевом разрыве).
2. Новая попытка создаёт новые [driver](glossary.md#term-driver)/[executors](glossary.md#term-executor), что приводит к истощению ресурсов.
3. Порты и соединения предыдущего драйвера могут оставаться заняты, препятствуя новой попытке.
4. Нет видимого мониторинга и cleanup подвисших процессов между попытками.

Кейс воспроизведения: запустить `IngestAppendOperator` с `retries > 0`, нарочно вызвать сбой (например, подключение к источнику разорвано). Проверить процессы Spark на scheduler — ожидается наличие старого [driver](glossary.md#term-driver) после [retry](glossary.md#term-retry). После нескольких [retry](glossary.md#term-retry) ресурсы исчерпаны.

#### Предпочтительный путь решения

Добавить `on_retry_callback` к `IngestAppendOperator` для явной очистки подвисших процессов перед повтором:

1. **Логирование [retry](glossary.md#term-retry)-события**: записать в meta-БД событие [retry](glossary.md#term-retry) для отслеживания истории попыток.
2. **Cleanup старых процессов**: определить и завершить процессы Spark предыдущей попытки ([driver](glossary.md#term-driver) и [executors](glossary.md#term-executor)) на scheduler, используя процессный ID из XCom.
3. **Сброс ресурсов**: освободить занятые порты и соединения.
4. **Сохранение state**: process_id DAG-рана остаётся неизменным; [retry](glossary.md#term-retry) использует тот же процесс-идентификатор для сквозного логирования.

##### Реализация и граница решения

- Этап 1: добавить `on_retry_callback` для логирования [retry](glossary.md#term-retry)-события в meta-БД.
- Этап 2: добавить cleanup процессов (параллелен работе над задачей [«Изолировать Spark drivers через DockerOperator»](#изолировать-spark-drivers-через-dockeroperator)). На DockerOperator управление контейнерами будет явным и встроенным.
- Текущее решение (без DockerOperator) требует ручного управления процессами; не гарантирует освобождение ресурсов в случае сбоя Airflow.
- Полное решение требует совместной работы с задачей [«Изолировать Spark drivers через DockerOperator»](#изолировать-spark-drivers-через-dockeroperator), где управление жизненным циклом контейнера обеспечит автоматический cleanup при сбое.

#### Проверки и критерии готовности

- [Unit](glossary.md#term-unit): `on_retry_callback` вызывает логирование [retry](glossary.md#term-retry)-события; исходящий process_id из XCom не меняется.
- [E2E](glossary.md#term-e2e): задача с `retries > 0` повторяется успешно; старые Spark процессы завершаются перед новой попыткой. Ресурсы (память, порты) освобождаются.
- Полный набор [e2e-тестов](glossary.md#term-e2e) (`./test/test.sh`) проходит; проверены сценарии единичной загрузки и параллельных загрузок с [retry](glossary.md#term-retry).
- Проверено отсутствие подвисших процессов на scheduler после завершения всех [retry](glossary.md#term-retry).

### Шаблонизация кода в generator/generate_dags.py

#### Контекст

Генератор DAG-ов (`ingestion/generator/generate_dags.py`) преобразует декларативные манифесты YAML в готовые Python-файлы DAG-ов. Функции `generate_dag_source`, `generate_snapshot_dag_source`, `_entity_block` и `_snapshot_entity_block` строят код строками, объединяемыми в список:

```python
lines = [
    "    with TaskGroup(...) as load_group:",
    "        log_start = TableLogOperator(...)",
]
```

Затем список объединяется в один файл через `"\n".join(lines)`.

SQL также встроен в Python, но используется по-разному:

- `ingestion/meta.py`: статические запросы PostgreSQL, включая многострочные INSERT/UPDATE реестра [инкремента](glossary.md#term-increment); значения передаются отдельно через DB-API-параметры `%s`. Транзакции, порядок блокировок и [CAS revision](glossary.md#term-cas) управляются Python-кодом.
- `ingestion/spark/raw/rdbms/reader.py`: `build_extract_query` собирает вложенный SELECT с проекциями и необязательным WHERE; `read_bounds` строит MIN/MAX по запросу чтения.
- `ingestion/iceberg.py::build_create_table_sql` и `ingestion/spark/raw/rdbms/incremental/load_act_snap.py::build_rtas_sql`: составные Spark SQL DDL для создания [raw](glossary.md#term-raw)-таблицы и атомарной замены актуального снимка.
- `ingestion/sources/rdbms/dialects/mssql.py` и `oracle.py`: короткие диалектные выражения, quoting идентификаторов и точное представление временных позиций. Это не самостоятельные запросы.

#### Проблема

1. **Хрупкость отступов**: форматирование пробелов и табуляции требует ручного контроля; легко допустить ошибку.
2. **Сложность модификации**: при добавлении новых операторов или параметров нужно изменять текстовые шаблоны; логика читается как строки, а не как объекты.
3. **Отсутствие структурной проверки при рендеринге**: строковая сборка сама по себе не проверяет синтаксис Python; такую проверку можно и нужно выполнять до записи файла. Шаблонизация сама по себе также не даёт типизации или гарантии корректности.
4. **Дублирование логики**: некоторые конструкции (например, TaskGroup, цепочки операторов) повторяются с незначительными вариациями.
5. **Смешение SQL и управляющего кода**: SQL приходится читать как набор строковых литералов, а изменение проекций, условий и DDL затрагивает Python-форматирование. При этом механическое вынесение каждого однострочного выражения лишь усложнит навигацию, а подстановка значений через Jinja вместо DB-API-параметров ухудшит безопасность.

Пример: `build_extract_query` соединяет вложенные SELECT, проекции и условный WHERE, а `plan_batch` содержит длинный INSERT с отдельным кортежем из 17 параметров. SQL-файлы облегчат чтение этих конструкций, но не отменят проверку соответствия параметров.

Вывод основан на статическом анализе перечисленных модулей; [runtime-кейс](glossary.md#term-runtime) и прототип шаблонизации в рамках оформления задачи не выполнялись.

#### Предпочтительный путь решения

Отделить тексты от управляющей логики:

- Jinja2 — только для шаблонов DAG.
- Обычные `.sql`-файлы — для выносимых SQL-запросов.
- Не использовать Jinja для SQL и не вводить `.sql.j2`.
- Не создавать AST-генератор или собственный язык шаблонов.
- Не переносить бизнес-логику в шаблоны.

##### Генерация DAG

- Вынести обычный и [snapshot](glossary.md#term-snapshot) DAG в `.py.j2` с общими фрагментами там, где это устраняет повторение. Python готовит контекст из проверенных манифестов; шаблон отвечает за текст, отступы и простые циклы/условия.
- Сохранить публичные функции генератора, имена файлов, конфигурационный контракт и полностью развёрнутые DAG-и: без YAML/Jinja-рендеринга при разборе DAG в Airflow.
- Сериализовать значения как корректные Python-литералы, а не вставлять необработанные строки; проверять результат через `ast.parse` или `compile` до записи. Сохранить детерминированность вывода и `--check`.

##### SQL-файлы и шаблоны

- Вынести полные статические запросы `ingestion/meta.py` в `.sql`, загружать их как текст без Jinja. Сохранить `%s` и отдельную передачу параметров в `execute`; не менять транзакции, порядок блокировок, [retry](glossary.md#term-retry)/[idempotency](glossary.md#term-idempotency) и [CAS revision](glossary.md#term-cas).
- Вынести составные запросы `build_extract_query`, `read_bounds`, `build_create_table_sql` и `build_rtas_sql` в `.sql` с именованными полями подстановки через стандартный `str.format_map`. Сохранить существующие функции как точки подготовки SQL-фрагментов и подстановки, без изменения их контрактов и семантики запросов. Необязательный WHERE и списки проекций готовить в Python, без условий и циклов в SQL-файлах.
- Оставить в Python валидацию, выбор диалекта, quoting идентификаторов, канонизацию и экранирование литералов, вычисление временных выражений, границ и предикатов. В именованные поля SQL-файлов подставлять только подготовленные фрагменты, а не произвольные непроверенные значения. Строковая подстановка не заменяет bind-параметры и не защищает от SQL injection; значения запросов pg-meta по-прежнему передаются отдельно через `%s`.
- Короткие запросы-проверки и диалектные выражения не выносить ради единообразия; не заменять DataFrame API SQL-шаблонами. SQL инициализации `conf/pg-meta/init/` не затрагивать.

##### Загрузка и окружение

- Хранить файлы рядом с владеющими компонентами, а не в общем несвязанном каталоге. Загружать независимо от текущего рабочего каталога; кэшировать загруженные тексты SQL и скомпилированные DAG-шаблоны в пределах процесса.
- Для Jinja в генераторе DAG использовать `StrictUndefined` и отключённый HTML autoescape. Для SQL ограничиться загрузкой текста и стандартной подстановкой подготовленных фрагментов; не строить универсальный SQL-конструктор.
- Явно объявить Jinja2 в окружении генератора DAG; для SQL новая зависимость не нужна. Включить `.sql`-файлы в доставку кода Airflow и Spark [driver](glossary.md#term-driver) в основном и тестовом контурах. Не полагаться на транзитивную зависимость Airflow или пути исходного checkout.

#### Проверки и критерии готовности

- Генерируемые DAG-ы имеют идентичный функциональный результат по сравнению с текущей реализацией.
- Все существующие [pipeline](glossary.md#term-pipeline)-конфигурации (`ingestion/conf/pipelines/*.yaml`) генерируются корректно.
- [Unit-тесты](glossary.md#term-unit) для генератора проходят; проверены граничные случаи (пустые entity-списки, специальные символы в именах).
- Выходной файл DAG-а корректен Python-синтаксис; проверка `python3 -m py_compile` успешна.
- Генерация/`--check` и полный `./test/test.sh` проходят.
- Проверены обычный и [snapshot](glossary.md#term-snapshot) DAG, корректные Python-литералы для кавычек, обратных слешей и специальных символов; отсутствующие переменные контекста приводят к явной ошибке.
- SQL: сохранены текстовая структура и порядок bind-параметров pg-meta, необязательный WHERE и проекции JDBC, MIN/MAX, технические колонки, location, свойства таблиц и атомарный [RTAS](glossary.md#term-rtas). Проверены PostgreSQL, MSSQL, Oracle и Spark SQL на соответствующих путях выполнения.
- Сохранены блокировки, [CAS revision](glossary.md#term-cas), фиксированное окно и повторное использование [batch](glossary.md#term-batch) на [retry](glossary.md#term-retry); временная точность и quoting специальных символов не изменились. Пробельные отличия допустимы, изменение семантики — нет.
- DAG-шаблоны доступны генератору, `.sql`-файлы — Airflow и Spark [driver](glossary.md#term-driver) вне корня репозитория; отсутствующий файл или именованное поле подстановки выдаёт понятную ошибку. Работа с SQL не требует Jinja; статические запросы загружаются без форматирования.
- Документация архитектуры обновлена по разделению логики и шаблонов, инструкция окружения — по зависимостям и доставке файлов. Реализация и тесты выполняются отдельной функциональной доработкой по спек-циклу; здесь изменяется только бэклог.

### Управление ресурсами Spark на уровне отдельной джобы

#### Контекст

Модель выполнения Spark (стенд использует Spark Standalone 3.5.5; дефолты параметров — по документации Apache Spark 3.5.x):

##### Выполнение и обмен данными

- [Driver](glossary.md#term-driver) создаёт SparkSession, строит план и распределяет задачи между [executors](glossary.md#term-executor).
  На него возвращаются результаты `collect` и агрегаций.
- [Executors](glossary.md#term-executor) выполняют задачи и записывают данные. При нехватке памяти
  промежуточные данные выгружаются на локальный диск — это **[spill](glossary.md#term-spill)**.
- На стенде [driver](glossary.md#term-driver) работает в `airflow-scheduler`, [executors](glossary.md#term-executor) — на `spark-worker`.
  Изоляция описана в [отдельной задаче](#изолировать-spark-drivers-через-dockeroperator).
- `filter`, `join`, `groupBy` строят план. Действия `count`, `show`, `write`
  запускают вычисления — **[job](glossary.md#term-job)**.
- [Job](glossary.md#term-job) делится на стадии (**[stages](glossary.md#term-job)**) по границам обмена данными.
  Стадия состоит из задач (**tasks**), каждая обрабатывает одну партицию.
- При стандартной настройке одна одновременно выполняемая задача занимает одно ядро [executor](glossary.md#term-executor).

**[Shuffle](glossary.md#term-shuffle)** — перераспределение данных для соединений, группировок и сортировки:

1. Map-задачи записывают блоки на диск [executors](glossary.md#term-executor).
2. Reduce-задачи забирают нужные блоки по сети.

Сеть и диск могут делать [shuffle](glossary.md#term-shuffle) дорогим. Возможные приёмы оптимизации самих джоб:

- **Broadcast join:** малая таблица копируется в память каждого [executor](glossary.md#term-executor).
  Большую таблицу не нужно перераспределять для соединения.
- **Key salting:** к ключам добавляют суффиксы, чтобы распределить
  слишком многочисленные строки одного ключа между несколькими партициями.

##### Память executor

Heap JVM включает:

- зарезервированную память Spark — около 300 МБ;
- user memory — структуры приложения и пользовательские функции;
- unified-область: storage для кэша и broadcast, execution для [shuffle](glossary.md#term-shuffle),
  соединений, сортировок и агрегаций.

Storage и execution динамически делят память. Execution может вытеснять кэш.

Управляемые параметры и их роль (в скобках — дефолты Spark 3.5.x):

| Параметр | Что задаёт | Дефолт |
|---|---|---|
| `spark.executor.cores` | число [слотов](glossary.md#term-slot) (одновременно выполняемых задач) в [executor](glossary.md#term-executor); в Standalone также определяет, сколько [executors](glossary.md#term-executor) приложения размещается на worker | не задан — в Standalone [executor](glossary.md#term-executor) получает все ядра worker |
| `spark.cores.max` | суммарное число ядер, которое приложение может занять на кластере Standalone; ограничивает приложение целиком, тогда как `spark.executor.cores` — один [executor](glossary.md#term-executor) | не задан — приложение получает все ядра кластера |
| `spark.executor.memory` | heap JVM [executor](glossary.md#term-executor); определяет, сколько данных обрабатывается без [spill](glossary.md#term-spill) | `1g` |
| `spark.sql.shuffle.partitions` | на сколько партиций делятся данные после [shuffle](glossary.md#term-shuffle); ориентир — 2–3 партиции на [слот](glossary.md#term-slot) | `200` |
| `spark.driver.memory` | heap JVM [driver](glossary.md#term-driver): хранит план и метаданные приложения, принимает результаты действий; вычисления и запись данных выполняют [executors](glossary.md#term-executor) | `1g` |

В эту задачу не входит внутреннее распределение памяти [executor](glossary.md#term-executor):

- `spark.memory.fraction=0.6` — доля unified-области после вычета зарезервированной памяти.
- `spark.memory.storageFraction=0.5` — защищённая от вытеснения доля storage внутри неё.

Эти настройки меняются только по результатам диагностики [spill](glossary.md#term-spill) или отказов памяти.

Ресурсы на стенде сейчас не заданы нигде: `conf/spark/spark-defaults.conf` содержит только настройки каталога Iceberg/Nessie/S3 и действует на все приложения кластера; `spark-worker` стартует без `--cores`/`--memory` и объявляет все ресурсы хоста; `IngestAppendOperator` передаёт в `SparkSubmitOperator` только application и deploy_mode, без `conf`. Существующий маршрут параметров джобы: `ingestion/conf/settings.yaml` (`spark.jdbc.num_partitions` — глобальный дефолт) и секция `spark:` [сущности](glossary.md#term-entity) манифеста (`jdbc_partitioning`) → генератор DAG-ов → оператор → аргументы джобы. Переименование выполнено, включая CLI `--no-jdbc-partitioning`, аргумент оператора и поле `EntityConfig`; повторное переименование не требуется.

Пример влияния настроек на [сущности](glossary.md#term-entity) DAG-а `ingest__rdbms__bank` (целевая конфигурация):

```yaml
entities:
  - fqn: retail_bank.transactions
    spark:
      jdbc_partitioning: true
      cores_max: 4
      executor_cores: 4
      executor_memory: 4g
      shuffle_partitions: 12
  - fqn: retail_bank.deals
    spark:
      jdbc_partitioning: false
      cores_max: 2
      executor_cores: 2
      executor_memory: 1g
```

Что изменится по сравнению с дефолтами Spark:

- spark-submit для `transactions` получит `--conf spark.cores.max=4 --conf spark.executor.cores=4 --conf spark.executor.memory=4g --conf spark.sql.shuffle.partitions=12`; для `deals` — аналогично своим значениям.
- Приложения ограничены 4 и 2 ядрами: при одновременном запуске ([пул](glossary.md#term-pool) `ingest_loads` допускает) они размещаются на единственном `spark-worker` одновременно, а не последовательно — сейчас первое приложение занимает все ядра worker.
- Heap [executor](glossary.md#term-executor) `transactions` — 4 ГБ вместо дефолтного 1 ГБ: больше данных обрабатывается без [spill](glossary.md#term-spill) на диск; `deals` остаётся на 1 ГБ.
- [Shuffle-стадия](glossary.md#term-shuffle) агрегации count/max в `transactions` использует 12 партиций вместо 200: меньше мелких задач и файлов данных.

#### Проблема

Все загрузки [raw-слоя](glossary.md#term-raw) исполняются с одинаковыми дефолтными ресурсами, и настроить их под конкретную джобу нельзя:

- heap [executor](glossary.md#term-executor) 1 ГБ: при обработке крупной таблицы возникает [spill](glossary.md#term-spill) на диск или отказ по памяти (OOM); увеличить его невозможно.
- Не заданные `spark.cores.max` и `spark.executor.cores`: каждое приложение запрашивает все ядра единственного worker одним [executor-ом](glossary.md#term-executor), и допущенные [пулом](glossary.md#term-pool) `ingest_loads` параллельные загрузки ожидают освобождения ядер вместо одновременного выполнения.
- `spark.sql.shuffle.partitions=200` не согласован с масштабом стенда: избыточные мелкие задачи и файлы; затрагивает и текущую загрузку (агрегация count/max порождает [stage](glossary.md#term-job) с [shuffle](glossary.md#term-shuffle)).

Пример: `retail_bank.transactions` (крупная таблица) и `retail_bank.deals` (малая, `spark.jdbc_partitioning: false`) получают идентичные ресурсы.

Кейс воспроизведения (не проверен в рамках оформления бэклога): запустить две загрузки одновременно и наблюдать Spark Master UI (`:8080`) — приложения, их [executors](glossary.md#term-executor), cores и memory; сравнить с последовательным запуском. Ожидаемое после решения — одновременное выполнение с заданными на уровне [сущности](glossary.md#term-entity) ресурсами вместо ожидания освобождения ядер.

#### Предпочтительный путь решения

Передавать ресурсы приложения по существующему маршруту:
манифест → генератор → оператор → spark-submit.
Не расширять `ingestion/conf/settings.yaml`.

1. Сохранить действующий контракт `spark.jdbc_partitioning` и `--no-jdbc-partitioning`: он управляет только JDBC-нарезкой, независимо от новых ресурсных ключей.
2. Добавить в секцию `spark:` [сущности](glossary.md#term-entity) ключи ресурсов: `cores_max`, `executor_cores`, `executor_memory`, `shuffle_partitions`, `driver_memory` (без префикса `spark.` — префикс добавляется при формировании `--conf`); валидация с белым списком допустимых ключей в `ingestion/generator/manifest.py`. [Сущность](glossary.md#term-entity) без ключей ресурсов запускается без `--conf` — с дефолтами Spark.
3. Генератор рендерит параметры в `IngestAppendOperator`; оператор передаёт их в `SparkSubmitOperator` через `conf` → `spark-submit --conf spark.<ключ>=<значение>`: конфигурация применяется к конкретному приложению и не затрагивает общий `spark-defaults.conf`.
4. Валидация значений: `cores_max`, `executor_cores` и `shuffle_partitions` — положительные целые; `executor_memory` и `driver_memory` — строки вида `<число><k|m|g>`.

Границы решения:

- Глобальные дефолты в `ingestion/conf/settings.yaml` не добавляются: `spark.jdbc.num_partitions` остаётся единственным глобальным параметром; ресурсы задаются только на уровне [сущностей](glossary.md#term-entity).
- Значения по [сущностям](glossary.md#term-entity) выбираются так, чтобы сумма ядер и памяти одновременно выполняемых загрузок из [пула](glossary.md#term-pool) `ingest_loads` помещалась в единственный `spark-worker`; [сущность](glossary.md#term-entity) без явных ресурсов запрашивает все ядра worker и блокирует параллельные приложения.
- `driver_memory` согласован с контейнером, где работает [driver](glossary.md#term-driver) (сейчас `airflow-scheduler`; после задачи [«Изолировать Spark drivers через DockerOperator»](#изолировать-spark-drivers-через-dockeroperator) — лимиты контейнера [driver](glossary.md#term-driver)).
- `shuffle_partitions` критичен прежде всего для будущих слоёв с join/groupBy; текущая [raw](glossary.md#term-raw)-загрузка порождает мало [shuffle](glossary.md#term-shuffle).
- Задача управляет только ресурсами запуска: JDBC-параллелизм чтения (`num_partitions`, `jdbc_partitioning`) — отдельный существующий механизм; broadcast-соединения, размножение ключей и управление числом партиций (repartition/coalesce) — приёмы реализации самих джоб, а не параметры запуска.

#### Проверки и критерии готовности

- [Unit](glossary.md#term-unit): белый список и форматы валидации ресурсных ключей без изменения `jdbc_partitioning`; оператор корректно формирует `--conf`-параметры из ключей [сущности](glossary.md#term-entity); для [сущности](glossary.md#term-entity) без ключей ресурсов `--conf` не передаётся.
- Генерация `python3 -m ingestion.generator.generate_dags --check` без расхождений после добавления ресурсов.
- [E2E](glossary.md#term-e2e): загрузка с нестандартными cores/memory/shuffle_partitions выполняется; параметры видны в Spark Master UI (`:8080`) и в команде spark-submit в логах задачи; полный `./test/test.sh` — `FAIL=0`.
- Схема секции `spark:` манифеста (ключ — тип — дефолт — назначение) обновлена в документации проекта по его правилам.

## Приоритет: можно отложить

### Автоматически формировать теги DAG и исключать дубликаты

#### Контекст

`ingestion/generator/generate_dags.py::generate_dag_source` передаёт в DAG только `pipeline.tags`. Например, `ingestion/conf/pipelines/bank/main.yaml` вручную задаёт `ingestion`, `bank`, `main`.

`generate_snapshot_dag_source` уже добавляет `act_snap`, идентификатор источника и нормализованное имя target-таблицы, затем `entity.act_snap.tags`. Общий парсер tags в `ingestion/generator/manifest.py` проверяет список непустых строк, но не удаляет повторы.

#### Проблема

Ручное повторение имени [pipeline](glossary.md#term-pipeline) и источника в тегах создаёт лишние изменения YAML и риск рассинхронизации после копирования или переименования файла. Общего обязательного тега загрузки нет; дополнительные теги могут дублировать автоматические или друг друга.

Пример: новый [pipeline](glossary.md#term-pipeline) без `tags` создаёт обычный DAG без тегов, хотя имя файла и источник известны генератору. [Snapshot](glossary.md#term-snapshot) с дополнительным тегом `act_snap` получает его повторно в генерируемом списке.

Вывод основан на статическом анализе генератора, парсера и манифеста; отображение и обработка повторов в работающем Airflow в рамках оформления задачи не проверялись.

#### Предпочтительный путь решения

Формировать теги при генерации DAG: автоматическая база плюс дополнительные теги соответствующей конфигурации, с детерминированным исключением дубликатов.

- Для обычного DAG автоматически добавлять имя исходного [pipeline](glossary.md#term-pipeline) YAML без расширения (`path.stem`), `source_id` и точный тег `ingest`, затем `pipeline.tags`. Под именем файла понимать YAML-манифест, а не сгенерированный `.py`; для `bank/main.yaml` база — `main`, `bank`, `ingest`.
- Для [snapshot](glossary.md#term-snapshot) DAG добавлять ту же базу исходного [pipeline](glossary.md#term-pipeline), затем существующие служебные `act_snap` и нормализованное имя target-таблицы, затем `incremental.act_snap.tags`. Дополнительные теги основного DAG не наследовать: настройки двух DAG остаются независимыми.
- Удалять точные повторы строк, сохраняя порядок первого появления, в том числе совпадение имени файла с `source_id` и совпадения дополнительных тегов с автоматическими. Не менять регистр и не нормализовать пользовательские теги неявно. Сохранить проверку списка непустых строк; при отсутствии `tags` использовать пустой список дополнений.
- Вынести сборку тегов в небольшую общую функцию генератора, без [runtime](glossary.md#term-runtime)-вычислений в Airflow и новых настроек. Не менять DAG/task IDs, расписания, Assets или обработку данных.
- Убрать из существующих YAML вручную перечисленные автоматические теги. Текущий общий маркер `ingestion` заменить на `ingest`; прочие осмысленные дополнительные теги сохранить. В документации явно отметить смену маркера для пользователей фильтров Airflow.
- Описать контракт автоматических и дополнительных тегов в документации генерации по правилам проекта. Учесть задачу шаблонизации DAG: правило сборки должно оставаться в Python-подготовке контекста, а не дублироваться в шаблонах.

Проверки и критерии готовности:

- [Unit](glossary.md#term-unit): автоматическая база без `tags`, дополнения, повторения внутри списка и пересечения с базой, одинаковые имя файла и источник; стабильный порядок повторной генерации.
- Обычный и [snapshot](glossary.md#term-snapshot) DAG получают общую базу; [snapshot](glossary.md#term-snapshot) сохраняет служебные теги и только собственные дополнения. Одинаковые имена файлов у разных источников дают соответствующие source-теги; вложенный путь не попадает в тег имени файла.
- Сгенерированные DAG содержат ожидаемые уникальные теги, корректно импортируются; генерация/`--check` успешны после обновления артефактов. В изолированном Airflow проверены теги и фильтрация по `ingest` без запуска загрузок ради проверки метаданных.
- Документация и примеры YAML согласованы; неприменимые и непроведённые проверки указаны явно. Здесь добавляется только задача бэклога, реализация выполняется отдельно по спек-циклу.

### Разделить пулы Airflow для загрузок и служебных задач

#### Контекст

Airflow [pools](glossary.md#term-pool) ограничивают допуск задач, но не выделяют CPU и память.
Генератор `ingestion/generator/generate_dags.py` создаёт для каждой таблицы
цепочку `log_start → load → log_end → publish_asset`.

- `airflow.pool.name` и `airflow.pool.slots` применяются только к `load`.
- Остальным задачам генератор сейчас не назначает отдельный служебный [пул](glossary.md#term-pool).

#### Проблема

Нет единой автоматической политики, разделяющей тяжёлые задачи загрузки и короткие служебные задачи. Без явного назначения служебного [пула](glossary.md#term-pool) их допуск определяется общими настройками Airflow, а не правилами фреймворка.

Пример целевого поведения: [сущность](glossary.md#term-entity) без настройки [пула](glossary.md#term-pool) отправляет `load` в общий [пул](glossary.md#term-pool) загрузок, а `log_start`, `log_end`, `publish_asset` — в отдельный служебный [пул](glossary.md#term-pool). Если для [сущности](glossary.md#term-entity) указан пользовательский [пул](glossary.md#term-pool), только `load` переходит в него; служебные задачи остаются в служебном [пуле](glossary.md#term-pool).

[Runtime-кейс](glossary.md#term-runtime) в рамках оформления задачи не проверялся. Для проверки назначения достаточно сравнить поля `pool` и `pool_slots` задач сгенерированного DAG-а; влияние заполнения [пулов](glossary.md#term-pool) на допуск нужно проверить в изолированном Airflow.

#### Предпочтительный путь решения

Настроить два штатных [пула](glossary.md#term-pool) фреймворка и автоматически назначать их в генераторе DAG-ов:

- Использовать `ingest_loads` как [пул](glossary.md#term-pool) по умолчанию для всех `load`, если [сущность](glossary.md#term-entity) не указала собственный `airflow.pool.name`.
- Создать отдельный [пул](glossary.md#term-pool) `ingest_service` для `log_start`, `log_end`, `publish_asset`; каждая такая задача занимает один [слот](glossary.md#term-slot). Число [слотов](glossary.md#term-slot) обоих [пулов](glossary.md#term-pool) определить при реализации с учётом лимитов стенда, не выдавая [пул](glossary.md#term-pool) за изоляцию вычислительных ресурсов.
- Явная настройка `airflow.pool.name` переопределяет только [пул](glossary.md#term-pool) `load`; `airflow.pool.slots` задаёт только вес `load`. Эти настройки не наследуются служебными задачами и не меняют их [пул](glossary.md#term-pool) или вес.
- Обеспечить [идемпотентное](glossary.md#term-idempotency) создание штатных [пулов](glossary.md#term-pool) при инициализации Airflow в основном и тестовом контурах. Пользовательские [пулы](glossary.md#term-pool) не создавать автоматически: они должны существовать до запуска соответствующей загрузки.
- Сохранить зависимости задач и публикацию Asset только после успешного `log_end`. Назначение [пулов](glossary.md#term-pool) для `generate_process_id` и `activate_source` не входит в эту задачу.
- Не использовать разделение [пулов](glossary.md#term-pool) как блокировку одновременной записи одной target-таблицы: это отдельный механизм взаимного исключения.

Проверки и критерии готовности:

- [Unit](glossary.md#term-unit): без переопределения `load` получает `ingest_loads`, все три служебные задачи — `ingest_service`; пользовательский [пул](glossary.md#term-pool) и число [слотов](glossary.md#term-slot) изменяют только `load`.
- Проверены создание обоих штатных [пулов](glossary.md#term-pool) и повторная инициализация без ошибок; документация описывает назначение, размеры [пулов](glossary.md#term-pool) и границы переопределения.
- [E2E](glossary.md#term-e2e): обычная загрузка и загрузка с пользовательским [пулом](glossary.md#term-pool) проходят; фактическое назначение задач совпадает с конфигурацией. При заполнении [пула](glossary.md#term-pool) загрузок готовые служебные задачи не блокируются именно этим [пулом](glossary.md#term-pool); общие лимиты [executor](glossary.md#term-executor)/scheduler по-прежнему могут ограничивать их исполнение.
- Генерация/`--check` и полный изолированный `./test/test.sh` проходят; архитектурная документация и инструкция настройки стенда обновлены по своим зонам ответственности.

### Убрать пароль JDBC-источника из аргументов spark-submit

#### Контекст

`IngestAppendOperator.execute()` в `ingestion/operators.py` получает реквизиты источника из Airflow Connection и передаёт пароль через `--jdbc-password` в `application_args` для `SparkSubmitOperator`. Загрузчики `ingestion/spark/raw/rdbms/full/load_entity.py` и `ingestion/spark/raw/rdbms/incremental/load_entity.py` принимают этот аргумент и используют его при JDBC-подключениях. Реквизиты pg-meta уже передаются переменными окружения `PG*`.

В закреплённом Spark provider 6.3.2 штатное логирование команды и сообщение об ошибке её выполнения используют маскирование аргументов с `password`/`secret`. Это не удаляет настоящий пароль из аргументов запущенного процесса. Источники: `conf/airflow/requirements.txt`, `IngestAppendOperator.execute()`, `parse_args()` загрузчика и `SparkSubmitHook._mask_cmd()` провайдера.

#### Проблема

Пароль JDBC-источника присутствует в командной строке spark-submit и доступен субъектам с соответствующими правами просмотра аргументов процессов контейнера. Маскирование штатной строки команды в task logs не устраняет этот канал раскрытия секрета. Отдельный вывод приложения или библиотек также не защищается одним лишь маскированием команды.

Пример: при запуске загрузки с тестовым паролем в реальных аргументах процесса присутствует `--jdbc-password <тестовый_пароль>`, хотя штатная строка `Spark-Submit cmd` должна отображать замаскированное значение.

Кейс воспроизведения ([runtime-проверка](glossary.md#term-runtime) не выполнялась): в изолированном тестовом контуре запустить загрузку с искусственным секретом и проверить наличие этого значения в аргументах процесса, не выводя реальные реквизиты. Наличие открытого пароля в фактических логах работающего стенда не проверено и не утверждается.

#### Предпочтительный путь решения

Передавать пароль источника через окружение процесса загрузчика вместо CLI-аргумента:

- Согласованно изменить `IngestAppendOperator` и загрузчик: убрать `--jdbc-password` из команды и обязательных CLI-параметров, передавать секрет отдельной переменной окружения, читать её при запуске приложения. Отсутствие обязательного секрета должно приводить к понятной ошибке без вывода его значения.
- Сохранить получение реквизитов из Airflow Connections во время исполнения задачи; не записывать пароль в манифесты, сгенерированные DAG-и, XCom или Spark conf. Передачу в JDBC выполнять из памяти приложения, без повторного формирования CLI с секретом.
- Не считать environment защищённым хранилищем: администратор контейнера или субъект с соответствующими правами может читать окружение. Решение устраняет присутствие секрета в аргументах процесса, но не заменяет управление доступом к среде выполнения.
- Выполнить эту задачу независимо от перехода на DockerOperator (задача [«Изолировать Spark drivers через DockerOperator»](#изолировать-spark-drivers-через-dockeroperator)); не расширять её до замены системы хранения секретов. При последующей смене режима запуска сохранить отсутствие пароля в командной строке.

Проверки и критерии готовности:

- [Unit](glossary.md#term-unit): в сформированных `application_args` нет аргумента пароля и его значения; секрет передаётся через окружение и корректно считывается загрузчиком; ошибка отсутствующей настройки не раскрывает секрет.
- В изолированном [e2e](glossary.md#term-e2e)-контуре успешная JDBC-загрузка сохранена, тестовый секрет отсутствует в аргументах процессов. Проверены штатный запуск и отказ подключения: пароль не появляется в task logs Airflow и диагностике загрузчика.
- Для проверок использовать искусственный секрет, не печатать реальные credentials. Область проверки логов и непроверенные каналы раскрытия указывать явно.
- Документация архитектуры обновлена по фактическому способу передачи реквизитов; [unit](glossary.md#term-unit) и полный изолированный [e2e](glossary.md#term-e2e) проходят.

### Выбирать JDBC-колонку по метрикам с управляемым повторным анализом

#### Контекст

Текущая эвристика в `ingestion/sources/rdbms/jdbc_partitioning.py` выбирает
целочисленный PK, затем целочисленный UNIQUE, затем дату, затем timestamp
через номер календарного дня. В ступенях 2–4 обязательна NOT NULL, а среди
кандидатов одной ступени выигрывает нормализованное имя. JDBC-чтение и
разведка MIN/MAX выполняются через `ingestion/spark/raw/rdbms/reader.py`.
Механика и разбор фактического лога находятся в разделе 6
`docs/architecture.md`.

Spark включает NULL в первую JDBC-партицию: это подтверждено предикатом
`"transaction_id" < 27502 or "transaction_id" is null` в предоставленном
логе запуска `manual__2026-11-09T15:54:58.294859+00:00` DAG-а
`ingest__rdbms__bank__main`. Это подтверждает механизм Spark, но не является
проверкой загрузки nullable-кандидата текущей эвристикой.

#### Проблема

Ограничение NOT NULL исключает колонки, по которым Spark способен прочитать
все строки. Выбор по имени не учитывает различимость значений и долю NULL
в окне чтения. При этом точный COUNT(DISTINCT) по нескольким кандидатам на
каждом запуске способен существенно увеличить нагрузку на источник.

Высокая различимость не гарантирует равномерность диапазонов, а большое
число NULL нагружает первую партицию. Нельзя считать предлагаемые метрики
гарантией ускорения или баланса потоков.

#### Предпочтительный путь решения

Проработать и реализовать единый выбор колонки по метрикам с управляемым
повторным анализом, не выполняя дорогое профилирование на каждой загрузке:

1. Снять обязательность NOT NULL при отборе кандидатов, сохранив обработку
   NULL первой партицией Spark и приоритет ступеней PK → UNIQUE → дата →
   timestamp-день. Проверить nullable UNIQUE отдельно для MSSQL и Oracle.
2. Если ступени PK и UNIQUE не дали результата и в очередной ступени есть
   несколько кандидатов, оценивать их в окне чтения по гранулярности
   `COUNT(DISTINCT col) / COUNT(col)`; максимизировать её. При равной
   гранулярности вычислять и максимизировать заполненность
   `COUNT(col) / COUNT(*)` только для претендентов на первое место.
   Обе метрики — до двух знаков после запятой. До реализации определить
   правила округления и сравнения, нулевых знаменателей и окончательного
   равенства. Для timestamp отдельно согласовать объект оценки: исходное
   значение или календарный день, по которому фактически строятся диапазоны.
3. В рамках проектирования проработать хранение выбранной колонки и
   результатов анализа в pg-meta: первичный анализ с последующим
   переиспользованием выбора — предложенное направление, не утверждённая
   схема хранения. Определить область ключа (источник, [сущность](glossary.md#term-entity), режим/окно),
   срок актуальности, условия и способ повторного запуска эвристики,
   инвалидирование при изменении схемы и поведение параллельных запусков.
   Конкретный период обновления сейчас не согласован.
4. Разделить переиспользуемый выбор колонки и текущие границы чтения:
   сохранённые MIN/MAX прошлого окна не должны ограничивать новое окно.
   Измерить стоимость профилирования на MSSQL и Oracle, зафиксировать
   политику отказов анализа и логировать причину выбора/переиспользования
   и актуальность статистики.

Проверки и критерии готовности:

- [Unit](glossary.md#term-unit): nullable-кандидаты, порядок ступеней, сравнение округлённых метрик,
  заполненность только при равенстве, пустое окно и полностью NULL-колонки.
- Интеграционные проверки pg-meta: сохранение выбора, переиспользование,
  повторный анализ, изменение схемы и конкурирующие обращения по
  согласованному контракту.
- [E2E](glossary.md#term-e2e) через реальные MSSQL/Oracle JDBC: NULL не теряются и не дублируются,
  окно сохраняется, сравниваются несколько кандидатов; повторный запуск
  не выполняет дорогое профилирование до наступления условия обновления.
- Документированы стоимость и ограничения метрик; полный изолированный
  `./test/test.sh` проходит. Новая эвристика и статистика сейчас не реализуются.
