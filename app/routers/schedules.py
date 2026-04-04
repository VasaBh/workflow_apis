import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel, field_validator

from app.database import get_db, doc_to_dict, docs_to_list
from app.dependencies import get_current_user, require_roles, CommonQueryParams
from app.response import success_response, error_response, paginate, paginate_meta

router = APIRouter(prefix="/v1/schedules", tags=["schedules"])


def validate_cron(cron_expression: str) -> bool:
    """Validate cron expression using croniter."""
    try:
        from croniter import croniter
        return croniter.is_valid(cron_expression)
    except Exception:
        return False


def get_next_run(cron_expression: str, timezone_str: str = "UTC") -> Optional[str]:
    """Get next run datetime from cron expression."""
    try:
        from croniter import croniter
        import zoneinfo
        try:
            tz = zoneinfo.ZoneInfo(timezone_str)
            now = datetime.now(tz)
        except Exception:
            now = datetime.now(timezone.utc)
        cron = croniter(cron_expression, now)
        return cron.get_next(datetime).isoformat()
    except Exception:
        return None


class CreateScheduleRequest(BaseModel):
    name: str
    blueprint_id: str
    cron_expression: str
    timezone: str = "UTC"
    context: dict = {}

    @field_validator("cron_expression")
    @classmethod
    def validate_cron_expr(cls, v):
        if not validate_cron(v):
            raise ValueError("Invalid cron expression")
        return v


class UpdateScheduleRequest(BaseModel):
    name: Optional[str] = None
    cron_expression: Optional[str] = None
    timezone: Optional[str] = None
    context: Optional[dict] = None

    @field_validator("cron_expression")
    @classmethod
    def validate_cron_expr(cls, v):
        if v is not None and not validate_cron(v):
            raise ValueError("Invalid cron expression")
        return v


@router.get("/")
async def list_schedules(
    commons: CommonQueryParams = Depends(),
    _: dict = Depends(get_current_user),
):
    db = get_db()
    query = {}
    if commons.search:
        query["name"] = {"$regex": commons.search, "$options": "i"}

    docs, total = await paginate(
        db["schedules"], query, commons.page, commons.limit,
        commons.sort, commons.sort_direction
    )
    return success_response(docs_to_list(docs), paginate_meta(commons.page, commons.limit, total))


@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_schedule(
    body: CreateScheduleRequest,
    current_user: dict = Depends(require_roles("admin", "editor")),
):
    db = get_db()

    existing = await db["schedules"].find_one({"name": body.name})
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=error_response("NAME_ALREADY_EXISTS", "A schedule with this name already exists"),
        )

    bp = await db["blueprints"].find_one({"_id": body.blueprint_id})
    if not bp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("BLUEPRINT_NOT_FOUND", "Blueprint not found"),
        )

    if bp.get("status") != "published":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("BLUEPRINT_NOT_PUBLISHED", "Blueprint must be published to create a schedule"),
        )

    now = datetime.now(timezone.utc).isoformat()
    next_run = get_next_run(body.cron_expression, body.timezone)
    schedule_id = str(uuid.uuid4())
    doc = {
        "_id": schedule_id,
        "name": body.name,
        "blueprint_id": body.blueprint_id,
        "cron_expression": body.cron_expression,
        "timezone": body.timezone,
        "context": body.context,
        "status": "active",
        "last_run_at": None,
        "next_run_at": next_run,
        "created_at": now,
        "updated_at": now,
        "created_by": current_user["id"],
    }

    await db["schedules"].insert_one(doc)
    return success_response(doc_to_dict(doc))


@router.get("/{schedule_id}")
async def get_schedule(
    schedule_id: str,
    _: dict = Depends(get_current_user),
):
    db = get_db()
    schedule = await db["schedules"].find_one({"_id": schedule_id})
    if not schedule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("SCHEDULE_NOT_FOUND", "Schedule not found"),
        )
    return success_response(doc_to_dict(schedule))


@router.put("/{schedule_id}")
async def update_schedule(
    schedule_id: str,
    body: UpdateScheduleRequest,
    current_user: dict = Depends(require_roles("admin", "editor")),
):
    db = get_db()
    schedule = await db["schedules"].find_one({"_id": schedule_id})
    if not schedule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("SCHEDULE_NOT_FOUND", "Schedule not found"),
        )

    if body.name is not None:
        existing = await db["schedules"].find_one({"name": body.name, "_id": {"$ne": schedule_id}})
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=error_response("NAME_ALREADY_EXISTS", "A schedule with this name already exists"),
            )

    updates = {"updated_at": datetime.now(timezone.utc).isoformat()}
    for field in ["name", "cron_expression", "timezone", "context"]:
        val = getattr(body, field)
        if val is not None:
            updates[field] = val

    if body.cron_expression or body.timezone:
        cron = updates.get("cron_expression", schedule["cron_expression"])
        tz = updates.get("timezone", schedule.get("timezone", "UTC"))
        updates["next_run_at"] = get_next_run(cron, tz)

    await db["schedules"].update_one({"_id": schedule_id}, {"$set": updates})
    updated = await db["schedules"].find_one({"_id": schedule_id})
    return success_response(doc_to_dict(updated))


@router.delete("/{schedule_id}")
async def delete_schedule(
    schedule_id: str,
    _: dict = Depends(require_roles("admin")),
):
    db = get_db()
    schedule = await db["schedules"].find_one({"_id": schedule_id})
    if not schedule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("SCHEDULE_NOT_FOUND", "Schedule not found"),
        )

    await db["schedules"].delete_one({"_id": schedule_id})
    return success_response({"message": "Schedule deleted successfully"})


@router.post("/{schedule_id}/activate")
async def activate_schedule(
    schedule_id: str,
    _: dict = Depends(require_roles("admin", "editor")),
):
    db = get_db()
    schedule = await db["schedules"].find_one({"_id": schedule_id})
    if not schedule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("SCHEDULE_NOT_FOUND", "Schedule not found"),
        )

    now = datetime.now(timezone.utc).isoformat()
    next_run = get_next_run(schedule["cron_expression"], schedule.get("timezone", "UTC"))
    await db["schedules"].update_one({"_id": schedule_id}, {"$set": {
        "status": "active",
        "next_run_at": next_run,
        "updated_at": now,
    }})
    updated = await db["schedules"].find_one({"_id": schedule_id})
    return success_response(doc_to_dict(updated))


@router.post("/{schedule_id}/deactivate")
async def deactivate_schedule(
    schedule_id: str,
    _: dict = Depends(require_roles("admin", "editor")),
):
    db = get_db()
    schedule = await db["schedules"].find_one({"_id": schedule_id})
    if not schedule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("SCHEDULE_NOT_FOUND", "Schedule not found"),
        )

    now = datetime.now(timezone.utc).isoformat()
    await db["schedules"].update_one({"_id": schedule_id}, {"$set": {
        "status": "paused",
        "updated_at": now,
    }})
    updated = await db["schedules"].find_one({"_id": schedule_id})
    return success_response(doc_to_dict(updated))


@router.post("/{schedule_id}/trigger")
async def trigger_schedule(
    schedule_id: str,
    current_user: dict = Depends(require_roles("admin", "executor")),
):
    from app.execution import execute_run
    import asyncio

    db = get_db()
    schedule = await db["schedules"].find_one({"_id": schedule_id})
    if not schedule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("SCHEDULE_NOT_FOUND", "Schedule not found"),
        )

    bp = await db["blueprints"].find_one({"_id": schedule["blueprint_id"]})
    if not bp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("BLUEPRINT_NOT_FOUND", "Blueprint not found"),
        )

    now = datetime.now(timezone.utc).isoformat()
    run_id = str(uuid.uuid4())
    run_doc = {
        "_id": run_id,
        "blueprint_id": schedule["blueprint_id"],
        "blueprint_version": bp.get("version", 1),
        "status": "not_started",
        "triggered_by": current_user["id"],
        "schedule_id": schedule_id,
        "context": schedule.get("context", {}),
        "started_at": None,
        "completed_at": None,
        "created_at": now,
        "updated_at": now,
    }
    await db["runs"].insert_one(run_doc)

    # Copy steps
    steps_cursor = db["steps"].find({"blueprint_id": schedule["blueprint_id"]}).sort("order", 1)
    steps = await steps_cursor.to_list(length=10000)
    for step in steps:
        step_run_doc = {
            "_id": str(uuid.uuid4()),
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
            "created_at": now,
        }
        await db["step_runs"].insert_one(step_run_doc)

    # Update schedule last_run_at
    next_run = get_next_run(schedule["cron_expression"], schedule.get("timezone", "UTC"))
    await db["schedules"].update_one({"_id": schedule_id}, {"$set": {
        "last_run_at": now,
        "next_run_at": next_run,
        "updated_at": now,
    }})

    asyncio.create_task(execute_run(run_id))

    return success_response(doc_to_dict(run_doc))


@router.get("/{schedule_id}/history")
async def get_schedule_history(
    schedule_id: str,
    commons: CommonQueryParams = Depends(),
    _: dict = Depends(get_current_user),
):
    db = get_db()
    schedule = await db["schedules"].find_one({"_id": schedule_id})
    if not schedule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("SCHEDULE_NOT_FOUND", "Schedule not found"),
        )

    docs, total = await paginate(
        db["runs"],
        {"schedule_id": schedule_id},
        commons.page, commons.limit,
        "created_at", -1,
    )
    return success_response(docs_to_list(docs), paginate_meta(commons.page, commons.limit, total))
