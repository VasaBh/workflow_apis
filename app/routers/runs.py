import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Optional, AsyncGenerator

from fastapi import APIRouter, HTTPException, status, Depends, BackgroundTasks, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.database import get_db, doc_to_dict, docs_to_list
from app.dependencies import get_current_user, require_roles, CommonQueryParams
from app.response import success_response, error_response, paginate, paginate_meta
from app.execution import execute_run
from app.notifications import notify_all_users, trigger_webhooks

router = APIRouter(prefix="/v1/runs", tags=["runs"])


class CreateRunRequest(BaseModel):
    blueprint_id: str
    context: dict = {}


@router.get("/")
async def list_runs(
    commons: CommonQueryParams = Depends(),
    blueprint_id: Optional[str] = Query(default=None),
    run_status: Optional[str] = Query(default=None, alias="status"),
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    _: dict = Depends(get_current_user),
):
    db = get_db()
    query = {}
    if blueprint_id:
        query["blueprint_id"] = blueprint_id
    if run_status:
        query["status"] = run_status
    if date_from or date_to:
        query["created_at"] = {}
        if date_from:
            query["created_at"]["$gte"] = date_from
        if date_to:
            query["created_at"]["$lte"] = date_to

    docs, total = await paginate(
        db["runs"], query, commons.page, commons.limit,
        commons.sort, commons.sort_direction
    )
    return success_response(docs_to_list(docs), paginate_meta(commons.page, commons.limit, total))


@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_run(
    body: CreateRunRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(require_roles("admin", "executor")),
):
    db = get_db()
    bp = await db["blueprints"].find_one({"_id": body.blueprint_id})
    if not bp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("BLUEPRINT_NOT_FOUND", "Blueprint not found"),
        )

    if bp.get("status") != "published":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("BLUEPRINT_NOT_PUBLISHED", "Blueprint must be published to create a run"),
        )

    now = datetime.now(timezone.utc).isoformat()
    run_id = str(uuid.uuid4())
    run_doc = {
        "_id": run_id,
        "blueprint_id": body.blueprint_id,
        "blueprint_version": bp.get("version", 1),
        "status": "not_started",
        "triggered_by": current_user["id"],
        "context": body.context,
        "started_at": None,
        "completed_at": None,
        "created_at": now,
        "updated_at": now,
    }
    await db["runs"].insert_one(run_doc)

    # Copy blueprint steps into step_runs
    steps_cursor = db["steps"].find({"blueprint_id": body.blueprint_id}).sort("order", 1)
    steps = await steps_cursor.to_list(length=10000)

    # Build mapping: step._id -> step_run_id (step_id field references blueprint step)
    for step in steps:
        step_run_id = str(uuid.uuid4())
        step_run_doc = {
            "_id": step_run_id,
            "run_id": run_id,
            "step_id": step["_id"],
            "name": step["name"],
            "type": step["type"],
            "parent_id": step.get("parent_id"),
            "order": step.get("order", 0),
            "script_id": step.get("script_id"),
            "script_params": step.get("script_params", {}),
            "entry": step.get("entry"),
            "dependencies": step.get("dependencies", []),
            "on_failure": step.get("on_failure", "block"),
            "retry_count": step.get("retry_count", 0),
            "timeout_seconds": step.get("timeout_seconds", 300),
            "status": "not_started",
            "output": None,
            "error": None,
            "logs": [],
            "started_at": None,
            "completed_at": None,
            "approved_by": None,
            "approved_at": None,
            "created_at": now,
        }
        await db["step_runs"].insert_one(step_run_doc)

    background_tasks.add_task(execute_run, run_id)

    return success_response(doc_to_dict(run_doc))


@router.get("/{run_id}")
async def get_run(
    run_id: str,
    _: dict = Depends(get_current_user),
):
    db = get_db()
    run = await db["runs"].find_one({"_id": run_id})
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("RUN_NOT_FOUND", "Run not found"),
        )

    run_dict = doc_to_dict(run)

    # Get step runs tree
    step_runs_cursor = db["step_runs"].find({"run_id": run_id}).sort("order", 1)
    all_step_runs = await step_runs_cursor.to_list(length=10000)
    all_step_runs = docs_to_list(all_step_runs)

    # Build tree
    step_map = {sr["id"]: sr for sr in all_step_runs}
    roots = []
    for sr in all_step_runs:
        sr["children"] = []

    for sr in all_step_runs:
        parent_id = sr.get("parent_id")
        if parent_id:
            parent_run = next((s for s in all_step_runs if s["step_id"] == parent_id), None)
            if parent_run:
                parent_run["children"].append(sr)
            else:
                roots.append(sr)
        else:
            roots.append(sr)

    run_dict["steps"] = roots
    return success_response(run_dict)


@router.delete("/{run_id}")
async def delete_run(
    run_id: str,
    _: dict = Depends(require_roles("admin")),
):
    db = get_db()
    run = await db["runs"].find_one({"_id": run_id})
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("RUN_NOT_FOUND", "Run not found"),
        )

    if run["status"] in ("in_progress",):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=error_response("RUN_IS_ACTIVE", "Cannot delete an active run"),
        )

    await db["runs"].delete_one({"_id": run_id})
    await db["step_runs"].delete_many({"run_id": run_id})
    return success_response({"message": "Run deleted successfully"})


@router.post("/{run_id}/pause")
async def pause_run(
    run_id: str,
    _: dict = Depends(require_roles("admin", "executor")),
):
    db = get_db()
    run = await db["runs"].find_one({"_id": run_id})
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("RUN_NOT_FOUND", "Run not found"),
        )

    if run["status"] != "in_progress":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("RUN_NOT_IN_PROGRESS", "Run is not in progress"),
        )

    now = datetime.now(timezone.utc).isoformat()
    await db["runs"].update_one({"_id": run_id}, {"$set": {"status": "paused", "updated_at": now}})
    updated = await db["runs"].find_one({"_id": run_id})
    return success_response(doc_to_dict(updated))


@router.post("/{run_id}/resume")
async def resume_run(
    run_id: str,
    background_tasks: BackgroundTasks,
    _: dict = Depends(require_roles("admin", "executor")),
):
    db = get_db()
    run = await db["runs"].find_one({"_id": run_id})
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("RUN_NOT_FOUND", "Run not found"),
        )

    if run["status"] != "paused":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("RUN_NOT_PAUSED", "Run is not paused"),
        )

    now = datetime.now(timezone.utc).isoformat()
    await db["runs"].update_one({"_id": run_id}, {"$set": {"status": "in_progress", "updated_at": now}})
    background_tasks.add_task(execute_run, run_id)
    updated = await db["runs"].find_one({"_id": run_id})
    return success_response(doc_to_dict(updated))


@router.post("/{run_id}/cancel")
async def cancel_run(
    run_id: str,
    _: dict = Depends(require_roles("admin", "executor")),
):
    db = get_db()
    run = await db["runs"].find_one({"_id": run_id})
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("RUN_NOT_FOUND", "Run not found"),
        )

    if run["status"] not in ("in_progress", "paused", "not_started"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("RUN_NOT_IN_PROGRESS", "Run cannot be cancelled in its current state"),
        )

    now = datetime.now(timezone.utc).isoformat()
    await db["runs"].update_one(
        {"_id": run_id},
        {"$set": {"status": "cancelled", "completed_at": now, "updated_at": now}},
    )

    # Cancel all not_started step_runs
    await db["step_runs"].update_many(
        {"run_id": run_id, "status": {"$in": ["not_started", "in_progress"]}},
        {"$set": {"status": "cancelled", "completed_at": now}},
    )

    await notify_all_users("run_cancelled", "Run cancelled", f"Run {run_id} was cancelled", reference_id=run_id)
    await trigger_webhooks("run_cancelled", {"run_id": run_id})

    updated = await db["runs"].find_one({"_id": run_id})
    return success_response(doc_to_dict(updated))


@router.post("/{run_id}/retry")
async def retry_run(
    run_id: str,
    background_tasks: BackgroundTasks,
    _: dict = Depends(require_roles("admin", "executor")),
):
    db = get_db()
    run = await db["runs"].find_one({"_id": run_id})
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("RUN_NOT_FOUND", "Run not found"),
        )

    if run["status"] != "failed":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("RUN_NOT_FAILED", "Run is not in failed state"),
        )

    now = datetime.now(timezone.utc).isoformat()
    # Reset failed step_runs to not_started
    await db["step_runs"].update_many(
        {"run_id": run_id, "status": "failed"},
        {"$set": {"status": "not_started", "started_at": None, "completed_at": None, "error": None, "output": None}},
    )

    await db["runs"].update_one(
        {"_id": run_id},
        {"$set": {"status": "in_progress", "updated_at": now}},
    )

    background_tasks.add_task(execute_run, run_id)
    updated = await db["runs"].find_one({"_id": run_id})
    return success_response(doc_to_dict(updated))


async def _sse_generator(run_id: str) -> AsyncGenerator[str, None]:
    """Generate SSE events for a run's step status updates."""
    db = get_db()
    terminal_states = {"completed", "failed", "cancelled"}
    last_states = {}

    while True:
        run = await db["runs"].find_one({"_id": run_id}, {"status": 1})
        if not run:
            yield f"data: {json.dumps({'error': 'Run not found'})}\n\n"
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
                })

        if updates:
            payload = {
                "run_id": run_id,
                "run_status": run["status"],
                "step_updates": updates,
            }
            yield f"data: {json.dumps(payload, default=str)}\n\n"

        if run["status"] in terminal_states:
            yield f"data: {json.dumps({'run_id': run_id, 'run_status': run['status'], 'done': True})}\n\n"
            break

        await asyncio.sleep(1)


@router.get("/{run_id}/stream")
async def stream_run(
    run_id: str,
    _: dict = Depends(get_current_user),
):
    db = get_db()
    run = await db["runs"].find_one({"_id": run_id})
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("RUN_NOT_FOUND", "Run not found"),
        )

    return StreamingResponse(
        _sse_generator(run_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
