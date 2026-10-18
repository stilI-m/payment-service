# Payment Processing Service

Асинхронный сервис процессинга платежей: FastAPI, Pydantic v2, SQLAlchemy 2.0 Async,
PostgreSQL, Alembic, RabbitMQ / FastStream, httpx, Python 3.11.

## Запуск

Нужны Docker и Docker Compose v2.

```bash
cp .env.example .env
# Измените API_KEY в .env, если сервис будет доступен другим пользователям.
docker compose up --build
```

API: http://localhost:8000; Swagger: http://localhost:8000/docs.
RabbitMQ Management: http://localhost:15672 (демонстрационные учётные данные `payments` / `payments`).
Порты опубликованы только на localhost. Пароли PostgreSQL/RabbitMQ в Compose — локальные демонстрационные значения.
Для внешнего размещения их нужно заменить согласованно с URL подключений.

API применяет миграции перед запуском. Consumer запускается после успешной проверки
API; оба процесса объявляют одинаковую RabbitMQ-топологию. PostgreSQL и RabbitMQ
хранят данные в именованных томах.

```bash
docker compose down  # остановить, сохранить данные
```

`docker compose down -v` удаляет данные **обоих** сервисов.

## Конфигурация

`API_KEY` берётся Compose из `.env` и передаётся API и consumer. Значение обязательно.
`DATABASE_URL` и `RABBITMQ_URL` внутри Compose заданы через имена сервисов `postgres`
и `rabbitmq`. В `.env.example` указаны эти же внутренние адреса; они не предназначены
для запуска Python с хоста. PostgreSQL наружу не опубликован.

При отдельном запуске Python задайте доступные ему адреса в переменных окружения.
Настройки приложения читаются из окружения или `.env`; окружение имеет приоритет.

## API

При отсутствии или неверном `X-API-Key` payment-endpoints и `/health` возвращают 401.
Swagger и OpenAPI доступны для просмотра документации без ключа.

### POST /api/v1/payments

Обязательные заголовки: `X-API-Key`, `Idempotency-Key` (1–255 символов).
Сумма положительная, не более 12 цифр всего и 2 после точки; валюты RUB, USD, EUR.
Описание: 1–500 символов. `metadata` по умолчанию `{}`. `webhook_url` — HTTP(S) URL.
Webhook указывает вызывающий клиент; используйте API-ключ только для доверенных клиентов.

```bash
export API_KEY='local-demo-key-change-me'  # то же значение, что в .env
curl -i http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" \
  -H 'Idempotency-Key: payment-001' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"2500.00","currency":"RUB","description":"Test payment","metadata":{"order_id":"order-001"},"webhook_url":"https://example.com/webhook"}'
```

Ответ — 202 Accepted:

```json
{"payment_id":"0b2a7da0-e709-45f9-bd45-695ada2edcfb","status":"pending","created_at":"2026-10-06T12:35:49Z"}
```

Повтор с тем же ключом возвращает тот же платёж и его **текущий** статус, также с 202.
Ключ идентифицирует первый запрос; другое тело с тем же ключом не изменяет платёж.
Ограничение UNIQUE в PostgreSQL и обработка конфликта при flush/commit защищают от
конкурентных запросов: создаются один Payment и одна запись Outbox.

Адрес example.com приведён для примера ошибки доставки. Для успешного webhook
укажите свой работающий обработчик; localhost внутри consumer означает сам контейнер.

### GET /api/v1/payments/{payment_id}

```bash
curl http://localhost:8000/api/v1/payments/0b2a7da0-e709-45f9-bd45-695ada2edcfb \
  -H "X-API-Key: $API_KEY"
```

Ответ 200 содержит `payment_id`, `amount`, `currency`, `description`, `metadata`,
`status`, `webhook_url`, `created_at`, `processed_at`.
Отсутствующий платёж — 404; неверный UUID или тело запроса — 422.

## Обработка и гарантии

```text
POST → PostgreSQL: Payment + Outbox в одной транзакции
     → Outbox publisher → RabbitMQ payments.new
     → Consumer → сохранение результата → webhook
                                         ↓ исчерпаны попытки
                              reject(requeue=False)
                                         ↓
                              payments.dlx → payments.dlq
```

Publisher каждые 2 секунды выбирает до 100 неопубликованных событий с
`FOR UPDATE SKIP LOCKED`. Сообщения persistent; включены publisher confirms и
ошибка при невозможности маршрутизации. `published_at` выставляется после публикации.
При ошибке транзакция откатывается, следующий проход повторяет отправку.

Это **at-least-once**: сбой после принятия сообщения брокером, но до commit Outbox
может дать дубликат. `message_id` равен ID Outbox; RabbitMQ сам дубликаты не удаляет.

Один consumer с `prefetch_count=1` обрабатывает сообщения последовательно.
Платёж блокируется `FOR UPDATE`, эмуляция длится 2–5 секунд, результат — 90%
`succeeded`, 10% `failed`. Результат и `processed_at` фиксируются до webhook.
При повторной доставке завершённый платёж не обрабатывается заново, но доставка
неотправленного webhook продолжается. После сохранённого успеха webhook повтор пропускается.

Webhook содержит `payment_id`, `status`, `processed_at`. Любой HTTP-ответ вне 2xx
или ошибка транспорта считается неудачей. Максимум **3 попытки всего**, с паузами
1 и 2 секунды перед второй и третьей. После успеха повторы прекращаются.
Счётчик `webhook_attempts` и отметка `webhook_delivered_at` хранятся в Payment.
Исчерпание попыток вызывает reject без requeue; DLX направляет сообщение в DLQ.
Успешное завершение обработчика вызывает ACK. Повтор сообщения из DLQ без сброса
исчерпанного бюджета снова приводит к reject, а не к новым HTTP-запросам.

Попытка резервируется в БД **до** HTTP-запроса. При аварии между резервированием и
отправкой она расходуется, даже если HTTP-запрос не начался. Это сохраняет верхнюю
границу трёх попыток при повторной доставке. Если получатель принял запрос, но
ответ или фиксация успеха потерялись, возможна повторная отправка. Получателю следует
обрабатывать уведомления идемпотентно по `payment_id`; exactly-once для HTTP не обещается.

Временные ошибки подключения/операций БД повторяются до трёх раз с новыми сессиями
и паузами 1/2 секунды на доставку сообщения. Они не сбрасывают HTTP-бюджет. Некорректные
сообщения и постоянные ошибки отклоняются сразу. После трёх ошибок БД — DLQ.

## Миграции

`70670276ec1f` создаёт payments/outbox и UNIQUE(idempotency_key).
`9c42a1d830be` добавляет состояние webhook. Существующие строки получают нулевой
счётчик; история webhook до этой миграции неизвестна. Поэтому старое завершённое
сообщение при повторной доставке может снова отправить уведомление.

При обновлении ранее запущенного проекта сохраните данные прежнего RabbitMQ:
добавление именованного тома не переносит содержимое старого анонимного тома.
Не удаляйте старый том до переноса или завершения обработки очереди.

## Проверки

Быстрые тесты (стандартный unittest, отдельный pytest не нужен):

```bash
python -m unittest discover -s tests -p test_unit.py -v
```

Полный тест использует отдельный Compose-проект, БД, брокер и локальный HTTP-приёмник.
Нужен Compose с поддержкой `!reset`. Он проверяет конкурентный POST, Outbox,
реальный consumer, webhook 1/2/3 попытки, DLQ, повторную доставку и восстановление
после сохранения результата. Тестовые сервисы не публикуют порты на хост.

```bash
API_KEY=integration-test-key docker compose -p mkk-bazis-test \
  -f docker-compose.yml -f compose.test.yml up --build \
  --abort-on-container-exit --exit-code-from tests
# Удалить только тестовые контейнеры и данные перед следующим полным прогоном:
API_KEY=integration-test-key docker compose -p mkk-bazis-test \
  -f docker-compose.yml -f compose.test.yml down -v
```

## Структура

- `app/api`, `app/schemas`: HTTP-контракт и валидация.
- `app/models`, `app/db`, `alembic`: модели, сессии, миграции.
- `app/services`: создание платежа, Outbox, webhook.
- `app/broker`, `app/consumer.py`: топология и один обработчик.
- `tests`: unit и интеграционные проверки.

`.env`, виртуальное окружение, IDE-файлы и кэши исключены из Git и Docker-контекста.
`.env.example` должен оставаться в репозитории.
