from faststream.rabbit import (
    Channel,
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)

from app.core.config import settings


DLX = RabbitExchange(
    "payments.dlx",
    type=ExchangeType.DIRECT,
    durable=True,
)

DLQ = RabbitQueue(
    "payments.dlq",
    routing_key="payments.failed",
    durable=True,
)

PAYMENTS_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.failed",
    },
)

broker = RabbitBroker(
    settings.rabbitmq_url,
    default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
)


async def declare_topology() -> None:
    dlx = await broker.declare_exchange(DLX)
    dlq = await broker.declare_queue(DLQ)
    await dlq.bind(exchange=dlx, routing_key="payments.failed")
    await broker.declare_queue(PAYMENTS_QUEUE)
