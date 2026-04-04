"""Notification and webhook delivery helpers."""
import hashlib
import hmac
import json
import uuid
from datetime import datetime, timezone
from typing import List, Optional

import httpx

from app.database import get_db, docs_to_list


VALID_EVENT_TYPES = [
    "run_started", "run_completed", "run_failed", "run_cancelled",
    "step_started", "step_completed", "step_failed", "step_skipped",
    "approval_required", "approval_approved", "approval_rejected",
]


async def create_notification(
    user_id: str,
    event_type: str,
    title: str,
    message: str,
    reference_id: Optional[str] = None,
):
    """Insert a notification document for a user."""
    db = get_db()
    now = datetime.now(timezone.utc).isoformat()
    doc = {
        "_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_type": event_type,
        "title": title,
        "message": message,
        "read": False,
        "created_at": now,
        "reference_id": reference_id,
    }
    await db["notifications"].insert_one(doc)
    return doc


async def notify_all_users(
    event_type: str,
    title: str,
    message: str,
    reference_id: Optional[str] = None,
    roles: Optional[List[str]] = None,
):
    """Notify all users (optionally filtered by roles)."""
    db = get_db()
    query = {}
    if roles:
        query["role"] = {"$in": roles}
    users_cursor = db["users"].find(query, {"_id": 1})
    users = await users_cursor.to_list(length=1000)
    for user in users:
        await create_notification(user["_id"], event_type, title, message, reference_id)


def _sign_payload(secret: str, payload: dict) -> str:
    """Generate HMAC-SHA256 signature for webhook payload."""
    body = json.dumps(payload, default=str).encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={sig}"


async def deliver_webhook(webhook: dict, event_type: str, data: dict):
    """Deliver a single webhook event."""
    payload = {
        "event": event_type,
        "data": data,
        "delivered_at": datetime.now(timezone.utc).isoformat(),
    }
    secret = webhook.get("secret", "")
    signature = _sign_payload(secret, payload)
    headers = {
        "Content-Type": "application/json",
        "X-WorkflowOS-Signature": signature,
        "X-WorkflowOS-Event": event_type,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(webhook["url"], json=payload, headers=headers)
            return resp.status_code
    except Exception:
        return None


async def trigger_webhooks(event_type: str, data: dict):
    """Find all active webhooks subscribed to this event and deliver."""
    db = get_db()
    webhooks_cursor = db["webhooks"].find({
        "active": True,
        "events": event_type,
    })
    webhooks = await webhooks_cursor.to_list(length=100)
    for wh in webhooks:
        await deliver_webhook(wh, event_type, data)
