# Настройка graphify — графа кодовой базы

## 1. Назначение

**graphify** строит граф кодовой базы (файлы, символы, зависимости между ними)
и отвечает на вопросы по нему консольными командами, не читая файлы целиком.
Граф используется агентом при анализе кода: поиск, навигация по зависимостям,
оценка влияния изменений — это сокращает расходы на LLM-анализ репозитория.
Правила работы агента с графом — в `AGENTS.md`, раздел «Граф кодовой базы».

Результат — `graphify-out/graph.json` в корне репозитория. Этот файл
**самодостаточен**: имена сообществ записаны прямо в узлы графа, поэтому после
`git pull` все запросы работают без перестроения и без ключей API. Остальные
файлы `graphify-out/` в git не попадают (раздел 6).

## 2. Установка утилиты

Утилита ставится через **pipx** — он сам создаёт изолированное виртуальное
окружение и выносит команду `graphify` в `~/.local/bin`. Отдельное виртуальное
окружение вручную не нужно: обычный `venv` потребовал бы активации при каждом
запуске или ручной правки `PATH`.

```bash
brew install pipx
pipx ensurepath
pipx install 'graphifyy[openai,sql]'
```

Обратите внимание: пакет на PyPI называется `graphifyy` (с двойной `y`),
устанавливаемая команда — `graphify`. Дополнение `openai` — доступ к
OpenAI-совместимому API, `sql` — разбор SQL-файлов (в репозитории есть
SQL и YAML, они обрабатываются всегда).

После установки перезапустите оболочку (или выполните `source ~/.zshrc`),
чтобы `~/.local/bin` попал в `PATH`. Проверка: `graphify --version`.

## 3. Переменные окружения

Ключи хранятся **вне репозитория** — в `~/.config/graphify/env`, чтобы их
нельзя было случайно закоммитить. Файл читается самим graphify только при
перестроении графа; запросы по готовому графу ключей не требуют.

```bash
mkdir -p ~/.config/graphify
cat > ~/.config/graphify/env <<'EOF'
export OPENAI_API_KEY="вставьте_ключ"
export OPENAI_BASE_URL="https://api.z.ai/api/coding/paas/v4"
export OPENAI_MODEL="glm-5.3"
EOF
chmod 600 ~/.config/graphify/env
```

| Переменная | Обязательна | Назначение |
|---|---|---|
| `OPENAI_API_KEY` | да | Ключ API. Без него семантическое извлечение и кластеризация не выполняются |
| `OPENAI_BASE_URL` | да | Адрес OpenAI-совместимого API (поддерживаются и внешние сервисы, и self-hosted серверы) |
| `OPENAI_MODEL` | да | Имя модели для семантического извлечения и именования сообществ |

Для разового использования в текущей оболочке файл можно подключить:
`source ~/.config/graphify/env`.

## 4. Перестроение графа и согласование

| Команда | Стоимость | Когда выполнять |
|---|---|---|
| `source ~/.config/graphify/env && graphify extract . --backend openai && graphify cluster-only` | LLM-API (платно) | Полное перестроение: семантические рёбра + кластеризация. Только с явного согласования |
| `graphify update .` | нет (без LLM) | Обновление после обычных правок кода: переизвлекает файлы, семантические рёбра не пересчитывает |

Обновлённый `graphify-out/graph.json` после согласованного перестроения
коммитится отдельным коммитом (файл ~1 МБ — история git растёт с каждой
пересборкой).

## 5. Git-хук перестроения (post-commit)

Хук живёт в `.git/hooks/post-commit` — это локальная настройка клона, в git
она не попадает. **По умолчанию хук ничего не делает**: перестроение графа
выполняется только с согласования.

| Режим | Как включить | Поведение |
|---|---|---|
| По умолчанию | — | После коммита ничего не происходит |
| Разовое согласование | `sh .git/hooks/post-commit force` | Одно перестроение немедленно |
| Автоперестроение | `touch ~/.config/graphify/auto-on-commit` | После каждого коммита; выключить — `rm ~/.config/graphify/auto-on-commit` |

Установка (однократно для клона):

```bash
cat > .git/hooks/post-commit <<'EOF'
#!/bin/sh
ENV_FILE="$HOME/.config/graphify/env"
GRAPHIFY="$HOME/.local/bin/graphify"
STATE_DIR="$HOME/.cache/graphify"
LOG_FILE="$STATE_DIR/post-commit.log"
LOCK_DIR="$STATE_DIR/post-commit.lock"
AUTO_FLAG="$HOME/.config/graphify/auto-on-commit"

if [ "${1:-}" != "force" ] && [ ! -f "$AUTO_FLAG" ]; then
    exit 0
fi

[ -f "$ENV_FILE" ] && [ -x "$GRAPHIFY" ] || exit 0

. "$ENV_FILE"

if [ -z "${OPENAI_API_KEY:-}" ]; then
    echo "[graphify] OPENAI_API_KEY пуст в $ENV_FILE — перестроение графа пропущено"
    exit 0
fi

mkdir -p "$STATE_DIR"

if [ -d "$LOCK_DIR" ]; then
    if [ $(( $(date +%s) - $(stat -f %m "$LOCK_DIR") )) -gt 1800 ]; then
        rm -rf "$LOCK_DIR"
    else
        echo "[graphify] предыдущий запуск ещё работает — пропуск"
        exit 0
    fi
fi

mkdir "$LOCK_DIR" 2>/dev/null || exit 0

(
    trap 'rmdir "$LOCK_DIR" 2>/dev/null' EXIT INT TERM
    cd "$(git rev-parse --show-toplevel)" || exit 1
    echo "=== $(date '+%Y-%m-%d %H:%M:%S') ${1:-commit} $(git rev-parse --short HEAD) ==="
    "$GRAPHIFY" extract . --backend openai && "$GRAPHIFY" cluster-only
) >>"$LOG_FILE" 2>&1 </dev/null &

exit 0
EOF
chmod +x .git/hooks/post-commit
```

Свойства хука: коммит никогда не блокируется (перестроение идёт в фоне);
лог — `~/.cache/graphify/post-commit.log`; пустой ключ — тихий пропуск;
новый запуск при незавершённом предыдущем пропускается (защита от серий
коммитов), «зависший» лок старше 30 минут снимается автоматически. Если
graphify установлен не в `~/.local/bin`, поправьте путь в `GRAPHIFY`.

## 6. Артефакты `graphify-out/` и git

В git отслеживается **только** `graphify-out/graph.json` — все команды
раздела 7 работают по нему одному. Правило в `.gitignore`:

```gitignore
graphify-out/*
!graphify-out/graph.json
```

| Артефакт | В git | Назначение |
|---|---|---|
| `graph.json` | да | Сам граф: узлы, рёбра, имена сообществ внутри узлов |
| `.graphify_labels.json` (+ `.sig`) | нет | Кэш кластеризации: стабильность номеров/имён сообществ между пересборками; запросам не нужен |
| `.graphify_analysis.json` | нет | Вход повторной кластеризации (`cluster-only`) и отчётов |
| `manifest.json` | нет | Кэш инкрементальности (хэши файлов): без него очередное `extract` просто просканирует всё заново |
| `graph.html`, `GRAPH_REPORT.md` | нет | Производные: визуализация и отчёт, регенерируются кластеризацией |
| `cache/`, `<дата>/` | нет | Семантический кэш и снимки запусков |
| `cost.json`, `memory/`, `reflections/` | нет | Учёт расходов API и петля обратной связи (`save-result`/`reflect`) |
| `.graphify_root`, `.graphify_semantic_marker`, `.cloud-cta-shown` | нет | Служебные метки утилиты |

## 7. Запросы по графу

Все команды работают офлайн, ключи API не нужны; граф по умолчанию —
`graphify-out/graph.json`.

| Команда | Назначение |
|---|---|
| `graphify query "<вопрос>"` | Обход графа по вопросу; `--budget N` — лимит токенов вывода (по умолчанию 2000), `--seed X` — старт с заданного узла |
| `graphify explain "<узел>"` | Узел и его связи (`--all` — все связи, по умолчанию 20 самых связных) |
| `graphify path "A" "B"` | Кратчайший путь между двумя узлами |
| `graphify affected "X"` | Обратный обход: что зависит от X (оценка влияния изменения) |
| `graphify god-nodes` | Самые связные узлы (архитектурные центры) |

Коды возврата: `0` — есть ответ; `3` — имя неоднозначно (команда перечислит
кандидатов); `4` — узел не найден.
