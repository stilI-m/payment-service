import asyncio
import os
import unittest
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')
os.environ.setdefault('RABBITMQ_URL', 'amqp://guest:guest@localhost/')
os.environ.setdefault('API_KEY', 'test-key')

import httpx
from fastapi import FastAPI
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError, OperationalError
from faststream.exceptions import RejectMessage
from app.api.payments import router
from app.main import health
from app.core.security import verify_api_key
from fastapi import Depends
from app.schemas.payment import PaymentCreate
from app.services.payment import PaymentService
from app.services import webhook
from app import consumer


def payment_data(**kw):
    data = dict(amount='10.00', currency='RUB', description='Test', webhook_url='https://example.com/hook')
    data.update(kw)
    return PaymentCreate(**data)


def session_for(payment):
    result = MagicMock()
    result.scalar_one.return_value = payment
    result.scalar_one_or_none.return_value = payment
    session = MagicMock()
    session.execute = AsyncMock(return_value=result)
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    return session


class ValidationTests(unittest.TestCase):
    def test_invalid_amounts(self):
        for amount in ['0', '-1', '0.001', '10000000000', 'NaN']:
            with self.subTest(amount=amount), self.assertRaises(ValidationError):
                payment_data(amount=amount)
        self.assertEqual(str(payment_data(amount='9999999999.99').amount), '9999999999.99')


class ApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_auth_and_header_validation(self):
        app = FastAPI()
        app.include_router(router)
        app.get('/health', dependencies=[Depends(verify_api_key)])(health)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            for path in ['/health', '/api/v1/payments/' + str(uuid.uuid4())]:
                self.assertEqual((await client.get(path)).status_code, 401)
                self.assertEqual((await client.get(path, headers={'X-API-Key': 'wrong'})).status_code, 401)
            self.assertEqual((await client.get('/health', headers={'X-API-Key': os.environ['API_KEY']})).status_code, 200)
            for key in ['', 'a' * 256]:
                result = await client.post('/api/v1/payments', json=payment_data().model_dump(mode='json'),
                                           headers={'X-API-Key': os.environ['API_KEY'], 'Idempotency-Key': key})
                self.assertEqual(result.status_code, 422)


class PaymentTests(unittest.IsolatedAsyncioTestCase):
    async def test_flush_race_returns_winner(self):
        session = session_for(None)
        cause = Exception('unique constraint')
        cause.constraint_name = 'payments_idempotency_key_key'
        original = Exception('duplicate')
        original.sqlstate = '23505'
        original.__cause__ = cause
        session.flush = AsyncMock(side_effect=IntegrityError('INSERT', {}, original))
        service = PaymentService(session)
        winner = object()
        service._get_by_idempotency_key = AsyncMock(side_effect=[None, winner])
        self.assertEqual(await service.create_payment(payment_data(), 'same'), (winner, False))
        session.rollback.assert_awaited_once()
        session.commit.assert_not_awaited()

    async def test_unrelated_integrity_error_not_swallowed(self):
        session = session_for(None)
        session.flush = AsyncMock(side_effect=IntegrityError('INSERT', {}, Exception('other')))
        service = PaymentService(session)
        service._get_by_idempotency_key = AsyncMock(return_value=None)
        with self.assertRaises(IntegrityError):
            await service.create_payment(payment_data(), 'same')
        session.rollback.assert_awaited_once()


class WebhookTests(unittest.IsolatedAsyncioTestCase):
    async def run_delivery(self, codes, attempts=0, delivered=False):
        payment = SimpleNamespace(id=uuid.uuid4(), status='succeeded', processed_at=datetime.now(timezone.utc),
                                  webhook_url='https://example.com/hook', webhook_attempts=attempts,
                                  webhook_delivered_at=datetime.now(timezone.utc) if delivered else None)
        session = session_for(payment)
        responses = [httpx.Response(c, request=httpx.Request('POST', payment.webhook_url)) for c in codes]
        client = MagicMock()
        client.post = AsyncMock(side_effect=responses)
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        sleep = AsyncMock()
        failed = False
        with patch.object(webhook, 'async_session_maker', return_value=session), \
             patch.object(webhook.httpx, 'AsyncClient', return_value=client), \
             patch.object(webhook.asyncio, 'sleep', sleep):
            try:
                await webhook.send_webhook(payment.id)
            except webhook.WebhookDeliveryError:
                failed = True
        return payment, client.post.await_count, [c.args[0] for c in sleep.await_args_list], failed

    async def test_three_failures(self):
        p, calls, delays, failed = await self.run_delivery([500, 500, 500])
        self.assertEqual((p.webhook_attempts, calls, delays, failed), (3, 3, [1, 2], True))

    async def test_success_stops_retries(self):
        p, calls, delays, failed = await self.run_delivery([500, 200])
        self.assertEqual((calls, delays, failed), (2, [1], False))
        self.assertIsNotNone(p.webhook_delivered_at)

    async def test_redelivery_resumes_budget(self):
        p, calls, delays, failed = await self.run_delivery([500], attempts=2)
        self.assertEqual((p.webhook_attempts, calls, delays, failed), (3, 1, [2], True))

    async def test_exhausted_and_delivered_do_not_send(self):
        self.assertEqual((await self.run_delivery([], attempts=3))[1:], (0, [], True))
        self.assertEqual((await self.run_delivery([], attempts=1, delivered=True))[1:], (0, [], False))


class ConsumerTests(unittest.IsolatedAsyncioTestCase):
    async def test_completed_payment_still_delivers_webhook(self):
        p = SimpleNamespace(id=uuid.uuid4(), status='succeeded')
        with patch.object(consumer, 'async_session_maker', return_value=session_for(p)), \
             patch.object(consumer, 'send_webhook', new_callable=AsyncMock) as send, \
             patch.object(consumer.asyncio, 'sleep', new_callable=AsyncMock) as sleep:
            await consumer.process_payment_once(p.id)
            send.assert_awaited_once_with(p.id)
            sleep.assert_not_awaited()

    async def test_db_errors_retry_three_times_then_reject(self):
        error = OperationalError('SELECT', {}, Exception('offline'))
        with patch.object(consumer, 'process_payment_once', new_callable=AsyncMock, side_effect=error) as run, \
             patch.object(consumer.asyncio, 'sleep', new_callable=AsyncMock) as sleep:
            with self.assertRaises(RejectMessage) as rejected:
                await consumer.process_payment({'payment_id': str(uuid.uuid4())})
            self.assertEqual(rejected.exception.extra_options, {'requeue': False})
            self.assertEqual(run.await_count, 3)
            self.assertEqual([c.args[0] for c in sleep.await_args_list], [1, 2])

    async def test_webhook_exhaustion_not_retried_by_outer_loop(self):
        with patch.object(consumer, 'process_payment_once', new_callable=AsyncMock,
                          side_effect=webhook.WebhookDeliveryError('exhausted')) as run:
            with self.assertRaises(webhook.WebhookDeliveryError):
                await consumer.process_payment({'payment_id': str(uuid.uuid4())})
            self.assertEqual(run.await_count, 1)
