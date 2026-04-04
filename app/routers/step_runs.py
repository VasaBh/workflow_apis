from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel
from datetime import datetime, timezone
from typing import Optional, List

from app.database import get_db, doc_to_dict, docs_to_list
from app.dependencies import get_current_user, require_roles, CommonQueryParams
from app.response import success_response, error_response, paginate, paginate_meta
from app.notifications import notify_all_users, trigger_webhooks

router = APIRouter(prefix="/v1/runs/{run_id}/steps", tags=["step_runs"])


class CompleteStepRequest(BaseModel):
    output: Optional[dict] = None
    notes: Optional[str] = None


class FailStepRequest(BaseModel):
    error: str
    notes: Optional[str] = None


class SkipStepRequest(BaseModel):
    reason: Optional[str] = None


class ApproveStepRequest(BaseModel):
    notes: Optional[str] = None


class RejectStepRequest(BaseModel):
    reason: str


async def get_run_or_404(db, run_id: str):
    run = await db["runs"].find_one({"_id": run_id})
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("RUN_NOT_FOUND", "Run not found"),
        )
    return run


async def get_step_run_or_404(db, run_id: str, step_id: str):
    step_run = await db["step_runs"].find_one({"run_id": run_id, "_id": step_id})
    if not step_run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("STEP_RUN_NOT_FOUND", "Step run not found"),
        )
    return step_run


async def _rollup_parent_status(db, run_id: str, parent_step_id: Optional[str]):
    """Roll up parent step run status based on children."""
    if not parent_step_id:
        return

    children_cursor = db["step_runs"].find({"run_id": run_id, "parent_id": parent_step_id})
    children = await children_cursor.to_list(length=1000)
    if not children:
        return

    statuses = [c["status"] for c in children]

    if all(s == "completed" for s in statuses):
        new_status = "completed"
    elif any(s == "failed" for s in statuses):
        new_status = "failed"
    elif any(s == "in_progress" for s in statuses):
        new_status = "in_progress"
    elif any(s == "blocked" for s in statuses):
        new_status = "blocked"
    elif any(s == "paused" for s in statuses):
        new_status = "paused"
    elif all(s == "skipped" for s in statuses):
        new_status = "skipped"
    else:
        new_status = "in_progress"

    now = datetime.now(timezone.utc).isoformat()
    updates = {"status": new_status}
    if new_status == "completed":
        updates["completed_at"] = now

    await db["step_runs"].update_one(
        {"run_id": run_id, "step_id": parent_step_id},
        {"$set": updates},
    )

    parent_step_run = await db["step_runs"].find_one({"run_id": run_id, "step_id": parent_step_id})
    if parent_step_run and parent_step_run.get("parent_id"):
        await _rollup_parent_status(db, run_id, parent_step_run["parent_id"])


async def _get_step_run_tree(db, run_id: str) -> List[dict]:
    """Build tree of step runs."""
    step_runs_cursor = db["step_runs"].find({"run_id": run_id}).sort("order", 1)
    all_srs = await step_runs_cursor.to_list(length=10000)
    all_srs = docs_to_list(all_srs)

    roots = []
    for sr in all_srs:
        sr["children"] = []

    for sr in all_srs:
        parent_id = sr.get("parent_id")
        if parent_id:
            parent = next((s for s in all_srs if s["step_id"] == parent_id), None)
            if parent:
                parent["children"].append(sr)
            else:
                roots.append(sr)
        else:
            roots.append(sr)

    return roots


@router.get("/")
async def list_step_runs(
    run_id: str,
    _: dict = Depends(get_current_user),
):
    db = get_db()
    await get_run_or_404(db, run_id)
    tree = await _get_step_run_tree(db, run_id)
    return success_response(tree)


@router.get("/{step_id}")
async def get_step_run(
    run_id: str,
    step_id: str,
    _: dict = Depends(get_current_user),
):
    db = get_db()
    await get_run_or_404(db, run_id)
    step_run = await get_step_run_or_404(db, run_id, step_id)
    step_dict = doc_to_dict(step_run)

    # Get children
    children_cursor = db["step_runs"].find({"run_id": run_id, "parent_id": step_run.get("step_id")}).sort("order", 1)
    children = await children_cursor.to_list(length=1000)
    step_dict["children"] = docs_to_list(children)
    return success_response(step_dict)


@router.post("/{step_id}/complete")
async def complete_step(
    run_id: str,
    step_id: str,
    body: CompleteStepRequest,
    current_user: dict = Depends(require_roles("admin", "executor")),
):
    db = get_db()
    await get_run_or_404(db, run_id)
    step_run = await get_step_run_or_404(db, run_id, step_id)

    if step_run["type"] not in ("manual",):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("NOT_MANUAL_STEP", "This endpoint is only for manual steps"),
        )

    if step_run["status"] not in ("blocked", "in_progress"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("STEP_NOT_IN_PROGRESS", "Step is not in a completable state"),
        )

    now = datetime.now(timezone.utc).isoformat()
    updates = {
        "status": "completed",
        "output": body.output,
        "completed_at": now,
    }
    if body.notes:
        updates["notes"] = body.notes

    await db["step_runs"].update_one({"_id": step_id}, {"$set": updates})

    # Roll up parent status
    if step_run.get("parent_id"):
        await _rollup_parent_status(db, run_id, step_run["parent_id"])

    await notify_all_users(
        "step_completed",
        f"Manual step completed: {step_run['name']}",
        f"Step '{step_run['name']}' was manually completed",
        reference_id=step_id,
    )
    await trigger_webhooks("step_completed", {"step_run_id": step_id, "run_id": run_id})

    updated = await db["step_runs"].find_one({"_id": step_id})
    return success_response(doc_to_dict(updated))


@router.post("/{step_id}/fail")
async def fail_step(
    run_id: str,
    step_id: str,
    body: FailStepRequest,
    current_user: dict = Depends(require_roles("admin", "executor")),
):
    db = get_db()
    await get_run_or_404(db, run_id)
    step_run = await get_step_run_or_404(db, run_id, step_id)

    if step_run["status"] not in ("blocked", "in_progress"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("STEP_NOT_IN_PROGRESS", "Step is not in a state that can be failed"),
        )

    now = datetime.now(timezone.utc).isoformat()
    await db["step_runs"].update_one({"_id": step_id}, {"$set": {
        "status": "failed",
        "error": body.error,
        "completed_at": now,
    }})

    if step_run.get("parent_id"):
        await _rollup_parent_status(db, run_id, step_run["parent_id"])

    await notify_all_users(
        "step_failed",
        f"Step failed: {step_run['name']}",
        f"Step '{step_run['name']}' was manually failed: {body.error}",
        reference_id=step_id,
    )
    await trigger_webhooks("step_failed", {"step_run_id": step_id, "run_id": run_id, "error": body.error})

    updated = await db["step_runs"].find_one({"_id": step_id})
    return success_response(doc_to_dict(updated))


@router.post("/{step_id}/skip")
async def skip_step(
    run_id: str,
    step_id: str,
    body: SkipStepRequest,
    current_user: dict = Depends(require_roles("admin")),
):
    db = get_db()
    await get_run_or_404(db, run_id)
    step_run = await get_step_run_or_404(db, run_id, step_id)

    if step_run["status"] in ("completed", "cancelled"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("CANNOT_SKIP_STEP", "Cannot skip a step that is already completed or cancelled"),
        )

    now = datetime.now(timezone.utc).isoformat()

    async def skip_recursive(sr_id: str, blueprint_step_id: str):
        await db["step_runs"].update_one({"_id": sr_id}, {"$set": {
            "status": "skipped",
            "completed_at": now,
        }})
        # Skip children
        children_cursor = db["step_runs"].find({"run_id": run_id, "parent_id": blueprint_step_id})
        children = await children_cursor.to_list(length=1000)
        for child in children:
            await skip_recursive(child["_id"], child.get("step_id", ""))

    await skip_recursive(step_id, step_run.get("step_id", ""))

    if step_run.get("parent_id"):
        await _rollup_parent_status(db, run_id, step_run["parent_id"])

    await notify_all_users(
        "step_skipped",
        f"Step skipped: {step_run['name']}",
        f"Step '{step_run['name']}' was skipped. Reason: {body.reason or 'Not specified'}",
        reference_id=step_id,
    )
    await trigger_webhooks("step_skipped", {"step_run_id": step_id, "run_id": run_id})

    updated = await db["step_runs"].find_one({"_id": step_id})
    return success_response(doc_to_dict(updated))


@router.post("/{step_id}/approve")
async def approve_step(
    run_id: str,
    step_id: str,
    body: ApproveStepRequest,
    current_user: dict = Depends(require_roles("admin", "executor")),
):
    db = get_db()
    await get_run_or_404(db, run_id)
    step_run = await get_step_run_or_404(db, run_id, step_id)

    if step_run["type"] != "approval":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("NOT_APPROVAL_STEP", "This endpoint is only for approval steps"),
        )

    if step_run["status"] not in ("blocked", "in_progress"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("STEP_NOT_IN_PROGRESS", "Step is not awaiting approval"),
        )

    now = datetime.now(timezone.utc).isoformat()
    await db["step_runs"].update_one({"_id": step_id}, {"$set": {
        "status": "completed",
        "approved_by": current_user["id"],
        "approved_at": now,
        "completed_at": now,
        "output": {"approved": True, "notes": body.notes},
    }})

    if step_run.get("parent_id"):
        await _rollup_parent_status(db, run_id, step_run["parent_id"])

    await notify_all_users(
        "approval_approved",
        f"Approval granted: {step_run['name']}",
        f"Step '{step_run['name']}' was approved by {current_user['name']}",
        reference_id=step_id,
    )
    await trigger_webhooks("approval_approved", {"step_run_id": step_id, "run_id": run_id, "approved_by": current_user["id"]})

    updated = await db["step_runs"].find_one({"_id": step_id})
    return success_response(doc_to_dict(updated))


@router.post("/{step_id}/reject")
async def reject_step(
    run_id: str,
    step_id: str,
    body: RejectStepRequest,
    current_user: dict = Depends(require_roles("admin", "executor")),
):
    db = get_db()
    await get_run_or_404(db, run_id)
    step_run = await get_step_run_or_404(db, run_id, step_id)

    if step_run["type"] != "approval":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("NOT_APPROVAL_STEP", "This endpoint is only for approval steps"),
        )

    if step_run["status"] not in ("blocked", "in_progress"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("STEP_NOT_IN_PROGRESS", "Step is not awaiting approval"),
        )

    now = datetime.now(timezone.utc).isoformat()
    await db["step_runs"].update_one({"_id": step_id}, {"$set": {
        "status": "failed",
        "approved_by": current_user["id"],
        "approved_at": now,
        "completed_at": now,
        "error": f"Rejected: {body.reason}",
        "output": {"approved": False, "reason": body.reason},
    }})

    if step_run.get("parent_id"):
        await _rollup_parent_status(db, run_id, step_run["parent_id"])

    await notify_all_users(
        "approval_rejected",
        f"Approval rejected: {step_run['name']}",
        f"Step '{step_run['name']}' was rejected by {current_user['name']}: {body.reason}",
        reference_id=step_id,
    )
    await trigger_webhooks("approval_rejected", {"step_run_id": step_id, "run_id": run_id, "rejected_by": current_user["id"]})

    updated = await db["step_runs"].find_one({"_id": step_id})
    return success_response(doc_to_dict(updated))


@router.get("/{step_id}/logs")
async def get_step_logs(
    run_id: str,
    step_id: str,
    commons: CommonQueryParams = Depends(),
    _: dict = Depends(get_current_user),
):
    db = get_db()
    await get_run_or_404(db, run_id)
    step_run = await get_step_run_or_404(db, run_id, step_id)

    logs = step_run.get("logs", [])
    total = len(logs)
    start = (commons.page - 1) * commons.limit
    end = start + commons.limit
    paginated_logs = logs[start:end]

    return success_response({"logs": paginated_logs}, {"page": commons.page, "limit": commons.limit, "total": total})
