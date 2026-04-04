"""Workflow execution engine."""
import asyncio
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from app.database import get_db, doc_to_dict
from app.sandbox import execute_script
from app.notifications import notify_all_users, trigger_webhooks


async def _update_step_run(db, step_run_id: str, updates: dict):
    await db["step_runs"].update_one({"_id": step_run_id}, {"$set": updates})


async def _rollup_parent_status(db, run_id: str, parent_id: Optional[str]):
    """Roll up step run status to parent based on children statuses."""
    if not parent_id:
        return

    children_cursor = db["step_runs"].find({"run_id": run_id, "parent_id": parent_id})
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
    elif all(s in ("not_started", "skipped") for s in statuses):
        new_status = "not_started"
    else:
        new_status = "in_progress"

    now = datetime.now(timezone.utc).isoformat()
    updates = {"status": new_status}
    if new_status == "completed":
        updates["completed_at"] = now
    elif new_status == "in_progress":
        updates["started_at"] = now

    await db["step_runs"].update_one(
        {"run_id": run_id, "step_id": parent_id},
        {"$set": updates},
    )

    # Recursively roll up to grandparent
    parent_step_run = await db["step_runs"].find_one({"run_id": run_id, "step_id": parent_id})
    if parent_step_run and parent_step_run.get("parent_id"):
        await _rollup_parent_status(db, run_id, parent_step_run["parent_id"])


async def _update_run_status(db, run_id: str):
    """Update the overall run status based on all top-level step_run statuses."""
    step_runs_cursor = db["step_runs"].find({"run_id": run_id, "parent_id": None})
    step_runs = await step_runs_cursor.to_list(length=1000)

    if not step_runs:
        return

    statuses = [sr["status"] for sr in step_runs]
    now = datetime.now(timezone.utc).isoformat()

    if all(s == "completed" for s in statuses):
        new_status = "completed"
    elif any(s == "failed" for s in statuses):
        new_status = "failed"
    elif any(s == "in_progress" for s in statuses):
        new_status = "in_progress"
    elif any(s == "blocked" for s in statuses):
        new_status = "in_progress"  # Run is still going, just waiting
    elif any(s == "paused" for s in statuses):
        new_status = "paused"
    elif all(s == "skipped" for s in statuses):
        new_status = "completed"
    else:
        new_status = "in_progress"

    updates = {"status": new_status, "updated_at": now}
    if new_status in ("completed", "failed", "cancelled"):
        updates["completed_at"] = now

    await db["runs"].update_one({"_id": run_id}, {"$set": updates})
    return new_status


async def _execute_step_run(db, step_run: dict, run_id: str):
    """Execute a single step run."""
    step_run_id = step_run["_id"]
    step_type = step_run["type"]
    now = datetime.now(timezone.utc).isoformat()

    await _update_step_run(db, step_run_id, {
        "status": "in_progress",
        "started_at": now,
    })

    await notify_all_users(
        "step_started",
        f"Step started: {step_run['name']}",
        f"Step '{step_run['name']}' has started in run {run_id}",
        reference_id=step_run_id,
    )
    await trigger_webhooks("step_started", {"step_run_id": step_run_id, "run_id": run_id})

    if step_type == "script":
        script_id = step_run.get("script_id")
        if not script_id:
            await _update_step_run(db, step_run_id, {
                "status": "failed",
                "error": "No script assigned to step",
                "completed_at": datetime.now(timezone.utc).isoformat(),
            })
            return "failed"

        script = await db["scripts"].find_one({"_id": script_id})
        if not script:
            await _update_step_run(db, step_run_id, {
                "status": "failed",
                "error": "Script not found",
                "completed_at": datetime.now(timezone.utc).isoformat(),
            })
            return "failed"

        timeout = step_run.get("timeout_seconds", 300)
        params = step_run.get("script_params", {})
        result = await execute_script(script["code"], script["entry"], params, timeout_seconds=min(timeout, 300))

        completion_time = datetime.now(timezone.utc).isoformat()
        if result["valid"]:
            await _update_step_run(db, step_run_id, {
                "status": "completed",
                "output": result.get("output"),
                "logs": result.get("logs", []),
                "completed_at": completion_time,
            })
            await notify_all_users(
                "step_completed",
                f"Step completed: {step_run['name']}",
                f"Step '{step_run['name']}' completed successfully",
                reference_id=step_run_id,
            )
            await trigger_webhooks("step_completed", {"step_run_id": step_run_id, "run_id": run_id})
            return "completed"
        else:
            on_failure = step_run.get("on_failure", "block")
            if on_failure == "retry" and step_run.get("retry_count", 0) > 0:
                # Simple retry: just run again up to retry_count times
                for attempt in range(step_run.get("retry_count", 0)):
                    result = await execute_script(
                        script["code"], script["entry"], params, timeout_seconds=min(timeout, 300)
                    )
                    if result["valid"]:
                        await _update_step_run(db, step_run_id, {
                            "status": "completed",
                            "output": result.get("output"),
                            "logs": result.get("logs", []),
                            "completed_at": datetime.now(timezone.utc).isoformat(),
                        })
                        return "completed"

            if on_failure == "skip":
                await _update_step_run(db, step_run_id, {
                    "status": "skipped",
                    "error": result.get("error"),
                    "logs": result.get("logs", []),
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                })
                await trigger_webhooks("step_skipped", {"step_run_id": step_run_id, "run_id": run_id})
                return "skipped"
            else:
                await _update_step_run(db, step_run_id, {
                    "status": "failed",
                    "error": result.get("error"),
                    "error_line": result.get("error_line"),
                    "logs": result.get("logs", []),
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                })
                await notify_all_users(
                    "step_failed",
                    f"Step failed: {step_run['name']}",
                    f"Step '{step_run['name']}' failed: {result.get('error')}",
                    reference_id=step_run_id,
                )
                await trigger_webhooks("step_failed", {"step_run_id": step_run_id, "run_id": run_id, "error": result.get("error")})
                return "failed"

    elif step_type in ("manual", "approval"):
        # Block until manually completed/approved
        status_val = "blocked"
        if step_type == "approval":
            await notify_all_users(
                "approval_required",
                f"Approval required: {step_run['name']}",
                f"Step '{step_run['name']}' requires approval in run {run_id}",
                reference_id=step_run_id,
                roles=["admin", "executor"],
            )

        await _update_step_run(db, step_run_id, {"status": status_val})
        return status_val
    else:
        # Unknown type, mark as failed
        await _update_step_run(db, step_run_id, {
            "status": "failed",
            "error": f"Unknown step type: {step_type}",
            "completed_at": datetime.now(timezone.utc).isoformat(),
        })
        return "failed"


async def execute_run(run_id: str):
    """Main execution engine. Runs steps in order respecting dependencies."""
    db = get_db()
    run = await db["runs"].find_one({"_id": run_id})
    if not run:
        return

    now = datetime.now(timezone.utc).isoformat()
    await db["runs"].update_one(
        {"_id": run_id},
        {"$set": {"status": "in_progress", "started_at": now}},
    )

    await notify_all_users(
        "run_started",
        "Workflow run started",
        f"Run {run_id} has started",
        reference_id=run_id,
    )
    await trigger_webhooks("run_started", {"run_id": run_id, "blueprint_id": run.get("blueprint_id")})

    # Get all step_runs ordered
    all_step_runs_cursor = db["step_runs"].find({"run_id": run_id}).sort("order", 1)
    all_step_runs = await all_step_runs_cursor.to_list(length=10000)

    completed_steps = set()
    failed_steps = set()
    skipped_steps = set()

    # Process step runs in topological/dependency order
    pending = list(all_step_runs)
    max_iterations = len(pending) * 2 + 10

    iteration = 0
    while pending and iteration < max_iterations:
        iteration += 1
        made_progress = False

        for step_run in list(pending):
            # Check if run was paused or cancelled
            current_run = await db["runs"].find_one({"_id": run_id}, {"status": 1})
            if current_run and current_run["status"] in ("paused", "cancelled"):
                return

            step_run_id = step_run["_id"]
            dependencies = step_run.get("dependencies", [])

            # Check if all dependencies are done
            deps_met = all(
                dep in completed_steps or dep in skipped_steps
                for dep in dependencies
            )

            if not deps_met:
                # Check if any dependency failed and on_failure is block
                any_dep_failed = any(dep in failed_steps for dep in dependencies)
                if any_dep_failed:
                    on_failure = step_run.get("on_failure", "block")
                    if on_failure == "skip":
                        await _update_step_run(db, step_run_id, {
                            "status": "skipped",
                            "completed_at": datetime.now(timezone.utc).isoformat(),
                        })
                        skipped_steps.add(step_run_id)
                        pending.remove(step_run)
                        made_progress = True
                    elif on_failure == "block":
                        await _update_step_run(db, step_run_id, {
                            "status": "blocked",
                        })
                        pending.remove(step_run)
                        failed_steps.add(step_run_id)
                        made_progress = True
                continue

            # Check parent is not failed/blocked
            parent_id = step_run.get("parent_id")
            if parent_id:
                parent_run = await db["step_runs"].find_one({"run_id": run_id, "step_id": parent_id})
                if parent_run and parent_run["status"] in ("failed", "skipped", "blocked", "cancelled"):
                    on_failure = step_run.get("on_failure", "block")
                    final_status = "skipped" if on_failure == "skip" else "blocked"
                    await _update_step_run(db, step_run_id, {
                        "status": final_status,
                        "completed_at": datetime.now(timezone.utc).isoformat(),
                    })
                    if final_status == "skipped":
                        skipped_steps.add(step_run_id)
                    else:
                        failed_steps.add(step_run_id)
                    pending.remove(step_run)
                    made_progress = True
                    continue

            # Execute step
            result_status = await _execute_step_run(db, step_run, run_id)

            if result_status in ("completed", "skipped"):
                if result_status == "completed":
                    completed_steps.add(step_run_id)
                else:
                    skipped_steps.add(step_run_id)
                pending.remove(step_run)
                made_progress = True

                # Roll up parent status
                if parent_id:
                    await _rollup_parent_status(db, run_id, parent_id)

            elif result_status == "failed":
                failed_steps.add(step_run_id)
                pending.remove(step_run)
                made_progress = True

                if parent_id:
                    await _rollup_parent_status(db, run_id, parent_id)

            elif result_status in ("blocked",):
                # Manual/approval steps: remove from pending, they'll be updated externally
                pending.remove(step_run)
                made_progress = True
                # Roll up parent status
                if parent_id:
                    await _rollup_parent_status(db, run_id, parent_id)

        if not made_progress:
            # Stuck — either all blocked/manual or circular deps
            await asyncio.sleep(2)

    # Determine final run status
    final_status = await _update_run_status(db, run_id)

    run_event = None
    if final_status == "completed":
        run_event = "run_completed"
        msg = f"Run {run_id} completed successfully"
    elif final_status == "failed":
        run_event = "run_failed"
        msg = f"Run {run_id} failed"
    else:
        msg = None

    if run_event:
        await notify_all_users(run_event, f"Workflow {final_status}", msg, reference_id=run_id)
        await trigger_webhooks(run_event, {"run_id": run_id})
