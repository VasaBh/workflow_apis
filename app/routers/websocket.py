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
    last_states = {}

    try:
        while True:
            current_run = await db["runs"].find_one({"_id": run_id}, {"status": 1})
            if not current_run:
                await websocket.send_json({"error": "Run not found"})
                break

            step_runs_cursor = db["step_runs"].find({"run_id": run_id})
            step_runs = await step_runs_cursor.to_list(length=10000)

            updates = []
            for sr in step_runs:
                sr_id = str(sr["_id"])
                current_status = sr.get("status")
                if last_states.get(sr_id) != current_status:
                    last_states[sr_id] = current_status
                    updates.append({
                        "step_run_id": sr_id,
                        "step_id": sr.get("step_id"),
                        "name": sr.get("name"),
                        "status": current_status,
                        "started_at": sr.get("started_at"),
                        "completed_at": sr.get("completed_at"),
                    })

            if updates:
                await websocket.send_json({
                    "run_id": run_id,
                    "run_status": current_run["status"],
                    "step_updates": updates,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })

            if current_run["status"] in terminal_states:
                await websocket.send_json({
                    "run_id": run_id,
                    "run_status": current_run["status"],
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
