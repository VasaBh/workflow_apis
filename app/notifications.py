"""Notification and webhook delivery helpers."""
import hashlib
import hmac
import json
import uuid
from datetime import datetime, timezone
from typing import Optional

import httpx

from app.database import get_db, docs_to_list

MAX_NOTIFICATIONS = 100

VALID_EVENT_TYPES = [
    "run_started", "run_completed", "run_failed", "run_cancelled",
    "step_started", "step_completed", "step_failed", "step_skipped",
    "approval_required", "approval_approved", "approval_rejected",
]


async def create_notification(
    event_type: str,
    title: str,
    message: str,
    reference_id: Optional[str] = None,
):
    """Insert a single broadcast notification document and enforce max 100 cap."""
    db = get_db()
    now = datetime.now(timezone.utc).isoformat()
    doc = {
        "_id": str(uuid.uuid4()),
        "event_type": event_type,
        "title": title,
        "message": message,
        "reference_id": reference_id,
        "read_by": [],
        "created_at": now,
    }
    await db["notifications"].insert_one(doc)

    # Enforce cap: delete oldest notifications beyond MAX_NOTIFICATIONS
    total = await db["notifications"].count_documents({})
    if total > MAX_NOTIFICATIONS:
        excess_cursor = (
            db["notifications"]
            .find({}, {"_id": 1})
            .sort("created_at", 1)
            .limit(total - MAX_NOTIFICATIONS)
        )
        excess = await excess_cursor.to_list(length=total - MAX_NOTIFICATIONS)
        if excess:
            ids = [d["_id"] for d in excess]
            await db["notifications"].delete_many({"_id": {"$in": ids}})

    return doc


async def notify_all_users(
    event_type: str,
    title: str,
    message: str,
    reference_id: Optional[str] = None,
    roles=None,  # kept for API compatibility, ignored (broadcast to all)
):
    """Broadcast a single notification to all users."""
    return await create_notification(event_type, title, message, reference_id)


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
