"""
Database factory — returns either a Motor (MongoDB) or File-system backend
depending on the DB env variable ("MONGO" or "FILES").

All routers use get_db() and the returned object is duck-typed to work with
both backends (same async interface).
"""

from app.config import settings
from typing import Optional

_client = None   # Motor client (MongoDB only)
_db = None       # Active database instance (either Motor DB or FileDatabase)


def get_db():
    """Return the active database backend (lazy-init, synchronous)."""
    global _client, _db
    if _db is None:
        if settings.DB.upper() == "FILES":
            from app.db.files import FileDatabase
            _db = FileDatabase(settings.FILES_DATA_DIR)
        else:
            from motor.motor_asyncio import AsyncIOMotorClient
            _client = AsyncIOMotorClient(settings.MONGODB_URL)
            _db = _client[settings.DATABASE_NAME]
    return _db


def get_collection(name: str):
    return get_db()[name]


async def add_indexes():
    """Create MongoDB indexes on startup. No-op for the file backend."""
    if settings.DB.upper() == "FILES":
        return

    db = get_db()

    await db["users"].create_index("email", unique=True)
    await db["scripts"].create_index("name", unique=True)
    await db["blueprints"].create_index("name", unique=True)
    await db["schedules"].create_index("name", unique=True)

    # schedule_locks: unique lock_key + TTL on expires_at
    await db["schedule_locks"].create_index("lock_key", unique=True)
    await db["schedule_locks"].create_index("expires_at", expireAfterSeconds=0)

    await db["step_runs"].create_index("run_id")
    await db["steps"].create_index("blueprint_id")
    await db["script_versions"].create_index("script_id")
    await db["notifications"].create_index("created_at")
    await db["runs"].create_index("blueprint_id")
    await db["runs"].create_index("status")

    # token_blacklist: unique token + TTL
    await db["token_blacklist"].create_index("token", unique=True)
    await db["token_blacklist"].create_index("expires_at", expireAfterSeconds=0)


async def close_db():
    """Close the database connection (MongoDB only)."""
    global _client, _db
    if _client is not None:
        _client.close()
        _client = None
    _db = None


# ---------------------------------------------------------------------------
# Document helpers
# ---------------------------------------------------------------------------

def doc_to_dict(doc: Optional[dict]) -> Optional[dict]:
    """Convert _id → id and ensure the document is a plain dict."""
    if doc is None:
        return None
    result = dict(doc)
    if "_id" in result:
        result["id"] = str(result.pop("_id"))
    return result


def docs_to_list(docs: list) -> list:
    return [doc_to_dict(d) for d in docs]
