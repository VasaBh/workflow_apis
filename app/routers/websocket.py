import asyncio
import json
from datetime import datetime, timezone

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query, status
from jose import JWTError, ExpiredSignatureError, jwt

from app.config import settings
from app.database import get_db, docs_to_list

router = APIRouter(tags=["websocket"])


async def _authenticate_ws(token: str) -> dict | None:
    """Authenticate WebSocket connection via JWT token. Returns user doc or None."""
    if not token:
        return None

    db = get_db()

    # Check blacklist
    blacklisted = await db["token_blacklist"].find_one({"token": token})
    if blacklisted:
        return None

    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET,
            algorithms=[settings.JWT_ALGORITHM],
        )
    except (JWTError, ExpiredSignatureError):
        return None

    if payload.get("type") != "access":
        return None

    user_id = payload.get("sub")
    if not user_id:
        return None

    user = await db["users"].find_one({"_id": user_id})
    return user


@router.websocket("/v1/ws/notifications")
async def websocket_notifications(
    websocket: WebSocket,
    token: str = Query(default=""),
):
    """Push new notifications to the connected user in real-time."""
    user = await _authenticate_ws(token)
    if not user:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await websocket.accept()
    db = get_db()
    user_id = user["_id"]

    # Track the newest notification seen so we only push new ones
    latest_cursor = db["notifications"].find({"user_id": user_id}).sort("created_at", -1).limit(1)
    latest = await latest_cursor.to_list(length=1)
    last_seen_at = latest[0]["created_at"] if latest else datetime.now(timezone.utc).isoformat()

    try:
        while True:
            new_cursor = db["notifications"].find({
                "user_id": user_id,
                "created_at": {"$gt": last_seen_at},
            }).sort("created_at", 1)
            new_notifications = await new_cursor.to_list(length=100)

            if new_notifications:
                last_seen_at = new_notifications[-1]["created_at"]
                unread_count = await db["notifications"].count_documents({
                    "user_id": user_id,
                    "read": False,
                })
                await websocket.send_json({
                    "type": "notifications",
                    "notifications": [
                        {
                            "id": n["_id"],
                            "event_type": n.get("event_type"),
                            "title": n.get("title"),
                            "message": n.get("message"),
                            "read": n.get("read", False),
                            "reference_id": n.get("reference_id"),
                            "created_at": n.get("created_at"),
                        }
                        for n in new_notifications
                    ],
                    "unread_count": unread_count,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })

            await asyncio.sleep(2)

    except WebSocketDisconnect:
        pass
    except Exception:
        try:
            await websocket.close()
        except Exception:
            pass


@router.websocket("/v1/ws/{run_id}")
async def websocket_run_stream(
    websocket: WebSocket,
    run_id: str,
    token: str = Query(default=""),
):
    db = get_db()
    user = await _authenticate_ws(token)

    if not user:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    run = await db["runs"].find_one({"_id": run_id})
    if not run:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await websocket.accept()

    terminal_states = {"completed", "failed", "cancelled"}
    last_run_status = None
    last_step_states: dict = {}

    try:
        while True:
            current_run = await db["runs"].find_one({"_id": run_id}, {"status": 1})
            if not current_run:
                await websocket.send_json({"error": "Run not found"})
                break

            current_run_status = current_run["status"]
            run_changed = current_run_status != last_run_status
            last_run_status = current_run_status

            step_runs_cursor = db["step_runs"].find({"run_id": run_id})
            step_runs = await step_runs_cursor.to_list(length=10000)

            step_updates = []
            for sr in step_runs:
                sr_id = str(sr["_id"])
                current_status = sr.get("status")
                if last_step_states.get(sr_id) != current_status:
                    last_step_states[sr_id] = current_status
                    step_updates.append({
                        "step_run_id": sr_id,
                        "step_id": sr.get("step_id"),
                        "name": sr.get("name"),
                        "status": current_status,
                        "started_at": sr.get("started_at"),
                        "completed_at": sr.get("completed_at"),
                    })

            if run_changed or step_updates:
                await websocket.send_json({
                    "run_id": run_id,
                    "run_status": current_run_status,
                    "step_updates": step_updates,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })

            if current_run_status in terminal_states:
                await websocket.send_json({
                    "run_id": run_id,
                    "run_status": current_run_status,
                    "done": True,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })
                break

            await asyncio.sleep(1)

    except WebSocketDisconnect:
        pass
    except Exception:
        try:
            await websocket.close()
        except Exception:
            pass
