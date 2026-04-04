import hashlib
import hmac
import json
import uuid
from datetime import datetime, timezone
from typing import List, Optional

import httpx
from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel, field_validator

from app.database import get_db, doc_to_dict, docs_to_list
from app.dependencies import require_roles, CommonQueryParams
from app.response import success_response, error_response, paginate, paginate_meta
from app.notifications import VALID_EVENT_TYPES

router = APIRouter(prefix="/v1/webhooks", tags=["webhooks"])


class CreateWebhookRequest(BaseModel):
    name: str
    url: str
    events: List[str]
    secret: Optional[str] = None
    active: bool = True

    @field_validator("events")
    @classmethod
    def validate_events(cls, v):
        invalid = [e for e in v if e not in VALID_EVENT_TYPES]
        if invalid:
            raise ValueError(f"Invalid event types: {', '.join(invalid)}. Valid: {', '.join(VALID_EVENT_TYPES)}")
        return v

    @field_validator("url")
    @classmethod
    def validate_url(cls, v):
        if not (v.startswith("http://") or v.startswith("https://")):
            raise ValueError("URL must start with http:// or https://")
        return v


class UpdateWebhookRequest(BaseModel):
    name: Optional[str] = None
    url: Optional[str] = None
    events: Optional[List[str]] = None
    secret: Optional[str] = None
    active: Optional[bool] = None

    @field_validator("events")
    @classmethod
    def validate_events(cls, v):
        if v is not None:
            invalid = [e for e in v if e not in VALID_EVENT_TYPES]
            if invalid:
                raise ValueError(f"Invalid event types: {', '.join(invalid)}")
        return v

    @field_validator("url")
    @classmethod
    def validate_url(cls, v):
        if v is not None and not (v.startswith("http://") or v.startswith("https://")):
            raise ValueError("URL must start with http:// or https://")
        return v


def _sign_payload(secret: str, payload: dict) -> str:
    body = json.dumps(payload, default=str).encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={sig}"


async def _deliver(url: str, payload: dict, secret: str = "", event_type: str = "test") -> tuple:
    """Returns (status_code, error_message)."""
    signature = _sign_payload(secret, payload) if secret else "sha256=unsigned"
    headers = {
        "Content-Type": "application/json",
        "X-WorkflowOS-Signature": signature,
        "X-WorkflowOS-Event": event_type,
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload, headers=headers)
            return resp.status_code, None
    except httpx.ConnectError:
        return None, "URL_NOT_REACHABLE"
    except httpx.TimeoutException:
        return None, "URL_NOT_REACHABLE"
    except Exception as e:
        return None, str(e)


@router.get("/")
async def list_webhooks(
    commons: CommonQueryParams = Depends(),
    _: dict = Depends(require_roles("admin")),
):
    db = get_db()
    query = {}
    if commons.search:
        query["name"] = {"$regex": commons.search, "$options": "i"}

    docs, total = await paginate(
        db["webhooks"], query, commons.page, commons.limit,
        commons.sort, commons.sort_direction
    )
    # Don't expose secret in list
    result = []
    for d in docs_to_list(docs):
        d.pop("secret", None)
        result.append(d)
    return success_response(result, paginate_meta(commons.page, commons.limit, total))


@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_webhook(
    body: CreateWebhookRequest,
    _: dict = Depends(require_roles("admin")),
):
    db = get_db()
    now = datetime.now(timezone.utc).isoformat()
    webhook_id = str(uuid.uuid4())
    secret = body.secret or str(uuid.uuid4()).replace("-", "")
    doc = {
        "_id": webhook_id,
        "name": body.name,
        "url": body.url,
        "events": body.events,
        "secret": secret,
        "active": body.active,
        "created_at": now,
        "updated_at": now,
    }
    await db["webhooks"].insert_one(doc)
    return success_response(doc_to_dict(doc))


@router.put("/{webhook_id}")
async def update_webhook(
    webhook_id: str,
    body: UpdateWebhookRequest,
    _: dict = Depends(require_roles("admin")),
):
    db = get_db()
    webhook = await db["webhooks"].find_one({"_id": webhook_id})
    if not webhook:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("WEBHOOK_NOT_FOUND", "Webhook not found"),
        )

    updates = {"updated_at": datetime.now(timezone.utc).isoformat()}
    for field in ["name", "url", "events", "secret", "active"]:
        val = getattr(body, field)
        if val is not None:
            updates[field] = val

    await db["webhooks"].update_one({"_id": webhook_id}, {"$set": updates})
    updated = await db["webhooks"].find_one({"_id": webhook_id})
    return success_response(doc_to_dict(updated))


@router.delete("/{webhook_id}")
async def delete_webhook(
    webhook_id: str,
    _: dict = Depends(require_roles("admin")),
):
    db = get_db()
    webhook = await db["webhooks"].find_one({"_id": webhook_id})
    if not webhook:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("WEBHOOK_NOT_FOUND", "Webhook not found"),
        )

    await db["webhooks"].delete_one({"_id": webhook_id})
    return success_response({"message": "Webhook deleted successfully"})


@router.post("/{webhook_id}/test")
async def test_webhook(
    webhook_id: str,
    _: dict = Depends(require_roles("admin")),
):
    db = get_db()
    webhook = await db["webhooks"].find_one({"_id": webhook_id})
    if not webhook:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("WEBHOOK_NOT_FOUND", "Webhook not found"),
        )

    payload = {
        "event": "test",
        "data": {},
        "webhook_id": webhook_id,
        "delivered_at": datetime.now(timezone.utc).isoformat(),
    }

    status_code, error = await _deliver(
        webhook["url"], payload, webhook.get("secret", ""), "test"
    )

    if error == "URL_NOT_REACHABLE":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("URL_NOT_REACHABLE", "Could not reach the webhook URL"),
        )

    if status_code is None or status_code >= 300:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("DELIVERY_FAILED", f"Webhook delivery failed with status {status_code}"),
        )

    return success_response({
        "message": "Test webhook delivered successfully",
        "status_code": status_code,
    })
