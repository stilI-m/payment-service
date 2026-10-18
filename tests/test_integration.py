"""Runs only in the isolated Compose test project; never use a working database."""
import asyncio
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading
import subprocess
import time
import unittest
import uuid

import aio_pika
import httpx
from sqlalchemy import func, select

from app.core.config import settings
from app.db.database import async_session_maker, engine
from app.models import Outbox, Payment


@unittest.skipUnless(os.getenv('RUN_INTEGRATION') == '1', 'isolated Docker integration run only')
class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_end_to_end(self):
        subprocess.run(['alembic', 'check'], check=True)
        counts = Counter()
        timestamps = {}
        bodies = {}

        class Hook(BaseHTTPRequestHandler):
            def do_POST(self):
                counts[self.path] += 1
                timestamps.setdefault(self.path, []).append(time.monotonic())
                bodies[self.path] = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                code = 500 if self.path == '/fail' or (self.path == '/retry' and counts[self.path] < 3) else 200
                self.send_response(code)
                self.end_headers()
            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(('0.0.0.0', 9000), Hook)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        connection = await aio_pika.connect_robust(settings.rabbitmq_url)
        try:
            channel = await connection.channel()
            dlq = await channel.declare_queue('payments.dlq', durable=True, passive=True)
            queue = await channel.declare_queue('payments.new', durable=True, passive=True)
            base = os.environ['TEST_WEBHOOK_BASE']
            async with httpx.AsyncClient(base_url=os.environ['TEST_API_URL'], timeout=20) as client:
                headers = {'X-API-Key': settings.api_key}
                self.assertEqual((await client.get('/health')).status_code, 401)
                self.assertEqual((await client.get('/health', headers=headers)).status_code, 200)
                self.assertEqual((await client.get('/api/v1/payments/' + str(uuid.uuid4()), headers=headers)).status_code, 404)

                async def create(path, key=None):
                    response = await client.post('/api/v1/payments', headers={**headers, 'Idempotency-Key': key or str(uuid.uuid4())},
                        json={'amount': '123.45', 'currency': 'RUB', 'description': 'Integration', 'webhook_url': base + path})
                    self.assertEqual(response.status_code, 202, response.text)
                    return uuid.UUID(response.json()['payment_id'])

                key = 'concurrent-' + str(uuid.uuid4())
                ids = await asyncio.gather(*(create('/ok', key) for _ in range(12)))
                self.assertEqual(len(set(ids)), 1)
                async with async_session_maker() as session:
                    self.assertEqual(await session.scalar(select(func.count()).select_from(Payment).where(Payment.idempotency_key == key)), 1)
                    self.assertEqual(await session.scalar(select(func.count()).select_from(Outbox).where(Outbox.payload['payment_id'].astext == str(ids[0]))), 1)

                async def wait_for(predicate, timeout=35):
                    end = time.monotonic() + timeout
                    while time.monotonic() < end:
                        if await predicate():
                            return
                        await asyncio.sleep(0.2)
                    self.fail('Timed out waiting for pipeline')

                async def delivered(payment_id):
                    async with async_session_maker() as session:
                        p = await session.get(Payment, payment_id)
                        return p.webhook_delivered_at is not None

                await wait_for(lambda: delivered(ids[0]))
                self.assertEqual(counts['/ok'], 1)
                await channel.default_exchange.publish(aio_pika.Message(json.dumps({'payment_id': str(ids[0])}).encode(),
                    content_type='application/json', delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key='payments.new')
                await asyncio.sleep(1)
                self.assertEqual(counts['/ok'], 1)

                retry_id = await create('/retry')
                await wait_for(lambda: delivered(retry_id))
                self.assertEqual(counts['/retry'], 3)
                times = timestamps['/retry']
                self.assertGreaterEqual(times[1] - times[0], 0.9)
                self.assertGreaterEqual(times[2] - times[1], 1.9)
                response = await client.get('/api/v1/payments/' + str(retry_id), headers=headers)
                self.assertEqual(response.status_code, 200)
                self.assertIn(response.json()['status'], ['succeeded', 'failed'])
                self.assertIsNotNone(response.json()['processed_at'])

                fail_id = await create('/fail')
                async def in_dlq():
                    await dlq.declare()
                    return dlq.declaration_result.message_count == 1
                await wait_for(in_dlq)
                dead = await dlq.get(no_ack=False, fail=False)
                self.assertEqual(json.loads(dead.body)['payment_id'], str(fail_id))
                self.assertEqual(dead.headers['x-death'][0]['reason'], 'rejected')
                self.assertEqual(dead.delivery_mode, aio_pika.DeliveryMode.PERSISTENT)
                await dead.ack()
                self.assertEqual(counts['/fail'], 3)
                async with async_session_maker() as session:
                    p = await session.get(Payment, fail_id)
                    self.assertEqual(p.webhook_attempts, 3)
                    self.assertIsNone(p.webhook_delivered_at)

                recovered_id = uuid.uuid4()
                async with async_session_maker() as session:
                    session.add(Payment(id=recovered_id, amount=Decimal('10'), currency='RUB', description='Recovery',
                        status='succeeded', idempotency_key=str(recovered_id), webhook_url=base + '/recovered',
                        created_at=datetime.now(timezone.utc), processed_at=datetime.now(timezone.utc), payment_metadata={}))
                    session.add(Outbox(event_type='payment.created', payload={'payment_id': str(recovered_id)},
                                      created_at=datetime.now(timezone.utc)))
                    await session.commit()
                await wait_for(lambda: delivered(recovered_id))
                self.assertEqual(counts['/recovered'], 1)
                self.assertEqual(bodies['/recovered']['status'], 'succeeded')
                async def drained():
                    await queue.declare()
                    async with async_session_maker() as session:
                        unpublished = await session.scalar(select(func.count()).select_from(Outbox).where(Outbox.published_at.is_(None)))
                    return queue.declaration_result.message_count == 0 and unpublished == 0
                await wait_for(drained)
        finally:
            await connection.close()
            await engine.dispose()
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
