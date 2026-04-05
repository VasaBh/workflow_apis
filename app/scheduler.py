"""APScheduler-based cron schedule runner with distributed locking."""
import asyncio
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.database import get_db

_scheduler: Optional[AsyncIOScheduler] = None
logger = logging.getLogger("workflowos.scheduler")


async def _try_acquire_lock(db, schedule_id: str, tick: str) -> bool:
    """Try to acquire a distributed lock for a schedule tick. Returns True if acquired."""
    lock_key = f"{schedule_id}:{tick}"
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=60)
    try:
        await db["schedule_locks"].insert_one({
            "_id": lock_key,
            "lock_key": lock_key,
            "schedule_id": schedule_id,
            "tick": tick,
            "acquired_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": expires_at,
        })
        return True
    except Exception:
        # Duplicate key — another pod already has the lock
        return False


async def _trigger_schedule(schedule: dict):
    """Trigger a run for the given schedule."""
    db = get_db()
    from app.execution import execute_run

    schedule_id = schedule["_id"]
    schedule_name = schedule.get("name", schedule_id)
    now = datetime.now(timezone.utc).isoformat()

    tz_str = schedule.get("timezone", "UTC")
    logger.info(
        "[scheduler] Triggering schedule '%s' (id=%s, timezone=%s, cron='%s')",
        schedule_name, schedule_id, tz_str, schedule.get("cron_expression"),
    )

    bp = await db["blueprints"].find_one({"_id": schedule["blueprint_id"]})
    if not bp or bp.get("status") != "published":
        logger.warning(
            "[scheduler] Skipping schedule '%s' — blueprint '%s' not found or not published",
            schedule_name, schedule.get("blueprint_id"),
        )
        return

    run_id = str(uuid.uuid4())
    logger.info(
        "[scheduler] Creating run %s for schedule '%s' (blueprint='%s')",
        run_id, schedule_name, bp.get("name"),
    )
    run_doc = {
        "_id": run_id,
        "blueprint_id": schedule["blueprint_id"],
        "blueprint_version": bp.get("version", 1),
        "status": "not_started",
        "triggered_by": "scheduler",
        "schedule_id": schedule["_id"],
        "context": schedule.get("context", {}),
        "started_at": None,
        "completed_at": None,
        "created_at": now,
        "updated_at": now,
    }
    await db["runs"].insert_one(run_doc)

    # Copy steps — respecting sequential flag
    steps_cursor = db["steps"].find({"blueprint_id": schedule["blueprint_id"]}).sort("order", 1)
    steps = await steps_cursor.to_list(length=10000)

    sequential = bp.get("sequential", False)
    if sequential:
        groups: dict = defaultdict(list)
        for step in steps:
            groups[step.get("parent_id")].append(step)
        seq_deps: dict = {}
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
            "created_at": now,
        }
        await db["step_runs"].insert_one(step_run_doc)

    # Update schedule last_run_at and compute next run in schedule's timezone
    from app.routers.schedules import get_next_run
    next_run = get_next_run(schedule["cron_expression"], tz_str)
    await db["schedules"].update_one({"_id": schedule["_id"]}, {"$set": {
        "last_run_at": now,
        "next_run_at": next_run,
        "updated_at": now,
    }})

    logger.info(
        "[scheduler] Run %s created for schedule '%s' with %d step(s) — next run at %s (%s)",
        run_id, schedule_name, len(steps), next_run, tz_str,
    )
    asyncio.create_task(execute_run(run_id))


async def check_due_schedules():
    """Check for schedules that are due to run."""
    db = get_db()
    now_str = datetime.now(timezone.utc).isoformat()

    schedules_cursor = db["schedules"].find({
        "status": "active",
        "next_run_at": {"$lte": now_str},
    })
    schedules = await schedules_cursor.to_list(length=100)

    if schedules:
        names = ", ".join(f"'{s.get('name', s['_id'])}'" for s in schedules)
        logger.info("[scheduler] Found %d due schedule(s) at %s: %s", len(schedules), now_str, names)

    for schedule in schedules:
        tick = schedule.get("next_run_at", now_str)
        acquired = await _try_acquire_lock(db, str(schedule["_id"]), tick)
        if acquired:
            await _trigger_schedule(schedule)
        else:
            logger.debug(
                "[scheduler] Lock not acquired for schedule '%s' (tick=%s) — skipping",
                schedule.get("name", schedule["_id"]), tick,
            )


def get_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler(timezone="UTC")
        _scheduler.add_job(
            check_due_schedules,
            "interval",
            seconds=30,
            id="check_schedules",
            replace_existing=True,
        )
    return _scheduler


def start_scheduler():
    scheduler = get_scheduler()
    if not scheduler.running:
        scheduler.start()


def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
