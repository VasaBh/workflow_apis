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


def _calc_progress(step_runs: list) -> dict:
    """Compute progress from a flat list of step_run dicts."""
    # Only count leaf steps (no children) to avoid double-counting parent groups
    parent_ids = {s.get("parent_id") for s in step_runs if s.get("parent_id")}
    leaf_runs = [sr for sr in step_runs if sr.get("step_id") not in parent_ids]
    total = len(leaf_runs)
    if total == 0:
        return {"total": 0, "completed": 0, "failed": 0, "skipped": 0, "in_progress": 0, "not_started": 0, "percentage": 0}
    completed  = sum(1 for s in leaf_runs if s.get("status") == "completed")
    failed     = sum(1 for s in leaf_runs if s.get("status") in ("failed", "blocked"))
    skipped    = sum(1 for s in leaf_runs if s.get("status") == "skipped")
    in_progress = sum(1 for s in leaf_runs if s.get("status") == "in_progress")
    not_started = sum(1 for s in leaf_runs if s.get("status") in ("not_started", "cancelled"))
    done = completed + failed + skipped
    return {
        "total": total,
        "completed": completed,
        "failed": failed,
        "skipped": skipped,
        "in_progress": in_progress,
        "not_started": not_started,
        "percentage": round(done / total * 100),
    }


async def _attach_blueprint_name(db, runs: list) -> list:
    """Add blueprint_name and progress to each run dict."""
    bp_cache: dict = {}
    for run in runs:
        bp_id = run.get("blueprint_id")
        if bp_id not in bp_cache:
            bp = await db["blueprints"].find_one({"_id": bp_id})
            bp_cache[bp_id] = bp.get("name") if bp else None
        run["blueprint_name"] = bp_cache[bp_id]
        run_id = run.get("id") or run.get("_id")
        step_runs_cursor = db["step_runs"].find({"run_id": run_id})
        step_runs = await step_runs_cursor.to_list(length=10000)
        run["progress"] = _calc_progress(step_runs)
    return runs


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
    runs = await _attach_blueprint_name(db, docs_to_list(docs))
    return success_response(runs, paginate_meta(commons.page, commons.limit, total))


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

    # For sequential blueprints, auto-derive dependencies from order within each parent group.
    # Each step (after the first in its group) depends on the previous step in the same group.
    sequential = bp.get("sequential", False)
    if sequential:
        from collections import defaultdict
        groups: dict = defaultdict(list)
        for step in steps:
            groups[step.get("parent_id")].append(step)
        # Groups are already sorted by order (steps cursor sorted by order)
        seq_deps: dict = {}  # step._id -> [prev_step._id]
        for group_steps in groups.values():
            for i, step in enumerate(group_steps):
                if i == 0:
                    seq_deps[step["_id"]] = step.get("dependencies", [])
                else:
                    prev_id = group_steps[i - 1]["_id"]
                    existing = list(step.get("dependencies", []))
                    if prev_id not in existing:
                        existing.append(prev_id)
                    seq_deps[step["_id"]] = existing
    else:
        seq_deps = {step["_id"]: step.get("dependencies", []) for step in steps}

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
            "dependencies": seq_deps[step["_id"]],
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
    bp = await db["blueprints"].find_one({"_id": run_dict["blueprint_id"]})
    run_dict["blueprint_name"] = bp.get("name") if bp else None

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

    run_dict["progress"] = _calc_progress(all_step_runs)
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
    """Generate SSE events for run and step status changes."""
    db = get_db()
    terminal_states = {"completed", "failed", "cancelled"}
    last_run_status = None
    last_step_states: dict = {}

    while True:
        run = await db["runs"].find_one({"_id": run_id}, {"status": 1})
        if not run:
            yield f"data: {json.dumps({'error': 'Run not found'})}\n\n"
            break

        current_run_status = run["status"]
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
                })

        # Emit whenever the run status OR any step status changes
        if run_changed or step_updates:
            payload = {
                "run_id": run_id,
                "run_status": current_run_status,
                "step_updates": step_updates,
                "progress": _calc_progress(step_runs),
            }
            yield f"data: {json.dumps(payload, default=str)}\n\n"

        if current_run_status in terminal_states:
            yield f"data: {json.dumps({'run_id': run_id, 'run_status': current_run_status, 'done': True})}\n\n"
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
