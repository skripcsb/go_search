## Быстрый запуск

```bash
docker compose up --build
```

Проверка:

```bash
curl http://localhost:8080/health
curl 'http://localhost:8080/api/v1/top?n=10'
```

Пример ответа:

```json
{
  "window_seconds": 300,
  "limit": 10,
  "items": [
    {"query": "iphone 15", "count": 2},
    {"query": "sneakers", "count": 1}
  ]
}
```

## HTTP API

- `GET /health` — проверка сервиса.
- `GET /readyz` — готовность Kafka consumer.
- `GET /api/v1/top?n=10` — Top-N запросов за окно.
- `GET /api/v1/stop-list` — текущий стоп-лист.
- `POST /api/v1/stop-list` — добавить слово в стоп-лист.
- `DELETE /api/v1/stop-list/{query}` — удалить слово из стоп-листа.
- `GET /metrics` — Prometheus-метрики.

## Контракт события Kafka

Топик по умолчанию: `search.events`.

```json
{
  "query": "iphone 15",
  "user_id": "u123",
  "request_id": "req-456",
  "timestamp": "2026-05-22T12:00:00Z",
  "source": "search-api"
}
```

Обязательное поле — `query`.

Поддерживаются также `event_id` и `occurred_at`.
Если `event_id` не передан, используется `request_id`.
Если `occurred_at` нет, берётся `timestamp` или текущее время сервиса.

## Архитектура

- `cmd/...` — запуск приложения и graceful shutdown.
- `internal/domain` — доменные типы и нормализация.
- `internal/application/trends` — бизнес-логика и порты.
- `internal/adapter/kafka` — Kafka consumer на `franz-go`.
- `internal/adapter/httpapi` — HTTP-ручки.
- `internal/infra/memorystore` — in-memory store со скользящим окном.
- `internal/metrics` — Prometheus-метрики.

Почему in-memory: это быстрый вариант для этого сценария. Чтений здесь намного больше, чем записей, поэтому данные держатся в памяти, а Top кешируется snapshot-ом.

## Защита от накруток

- `MAX_QUERY_PER_BUCKET` — ограничение одинаковых запросов в одном бакете.
- Дедупликация по `event_id` и `session_id+query`.
- Динамический стоп-лист через API.

## Конфигурация

- `HTTP_ADDR` — адрес HTTP-сервера, по умолчанию `:8080`
- `KAFKA_BROKERS` — брокеры Kafka, по умолчанию `localhost:9092`
- `KAFKA_TOPIC` — топик, по умолчанию `search.events`
- `KAFKA_GROUP_ID` — consumer group, по умолчанию `search-trends`
- `WINDOW` — размер окна, по умолчанию `5m`
- `BUCKET_RESOLUTION` — размер бакета, по умолчанию `1s`
- `MAX_QUERY_PER_BUCKET` — лимит одинаковых запросов в бакет, по умолчанию `1000`
- `STOPLIST_FILE` — файл стоп-листа, по умолчанию `stoplist.json`

## Локальная проверка

```bash
go test ./internal/infra/memorystore -run Test
go test ./internal/adapter/httpapi -run TestHTTPTopIntegration -v
go test ./...
```

## Производительность

В проекте есть бенчмарки для хранилища:

```bash
go test -run=^$ -bench=. -benchmem -benchtime=2s ./internal/infra/memorystore
```

## Скрипты

- `scripts/produce.sh` — публикует тестовые события в Kafka.
- `scripts/loadtest.sh` — публикует события и затем бьёт `/api/v1/top` через `hey`.

Пример:

```bash
chmod +x scripts/*.sh
./scripts/produce.sh 50000 100
EVENTS=50000 UNIQUE=100 DURATION=60s CONCURRENCY=200 ./scripts/loadtest.sh
```

## Ограничения

- Хранилище и стоп-лист живут в памяти; после рестарта данные окна теряются.
- Антифрод здесь базовый: лимит по бакету и дедупликация. Для production это обычно расширяют.
