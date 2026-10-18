import asyncio
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from app.api.payments import router as payment_router
from app.broker.rabbit import broker, declare_topology
from app.core.security import verify_api_key
from app.db.database import engine
from app.services.outbox import outbox_publisher_loop


@asynccontextmanager
async def lifespan(app: FastAPI):
    await broker.connect()
    try:
        await declare_topology()
        publisher_task = asyncio.create_task(outbox_publisher_loop())
        try:
            yield
        finally:
            publisher_task.cancel()
            try:
                await publisher_task
            except asyncio.CancelledError:
                pass
    finally:
        await broker.stop()
        await engine.dispose()


app = FastAPI(title="Payment Processing Service", lifespan=lifespan)
app.include_router(payment_router)


@app.get("/health", dependencies=[Depends(verify_api_key)])
async def health():
    return {"status": "ok"}
