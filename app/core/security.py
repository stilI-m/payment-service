import secrets

from fastapi import Header, HTTPException, status

from app.core.config import settings


async def verify_api_key(key: str | None = Header(default=None, alias="X-API-Key")):
    if key is None or not secrets.compare_digest(key.encode(), settings.api_key.encode()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
