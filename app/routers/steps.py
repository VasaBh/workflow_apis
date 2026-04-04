from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel
from datetime import datetime, timezone
from typing import Optional, List
import uuid

from app.database import get_db, doc_to_dict, docs_to_list
from app.dependencies import get_current_user, require_roles
from app.response import success_response, error_response

router = APIRouter(prefix="/v1/blueprints/{blueprint_id}/steps", tags=["steps"])

VALID_STEP_TYPES = ["manual", "script", "approval"]
VALID_ON_FAILURE = ["block", "skip", "retry"]


class CreateStepRequest(BaseModel):
    name: str
    type: str = "manual"
    parent_id: Optional[str] = None
    order: int = 0
    script_id: Optional[str] = None
    script_params: dict = {}
    entry: Optional[str] = None
    dependencies: List[str] = []
    on_failure: str = "block"
    retry_count: int = 0
    timeout_seconds: int = 300
    validation_rules: dict = {}


class UpdateStepRequest(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    script_id: Optional[str] = None
    script_params: Optional[dict] = None
    entry: Optional[str] = None
    dependencies: Optional[List[str]] = None
    on_failure: Optional[str] = None
    retry_count: Optional[int] = None
    timeout_seconds: Optional[int] = None
    validation_rules: Optional[dict] = None


class ReorderRequest(BaseModel):
    step_ids: List[str]


async def get_step_tree(db, blueprint_id: str, parent_id: Optional[str] = None) -> List[dict]:
    """Build step tree for a given parent."""
    query = {"blueprint_id": blueprint_id, "parent_id": parent_id}
    steps_cursor = db["steps"].find(query).sort("order", 1)
    steps = await steps_cursor.to_list(length=1000)
    steps = docs_to_list(steps)

    for step in steps:
        step["children"] = await get_step_tree(db, blueprint_id, step["id"])

    return steps


async def check_blueprint_published(db, blueprint_id: str):
    """Raise exception if blueprint is published."""
    bp = await db["blueprints"].find_one({"_id": blueprint_id})
    if not bp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("BLUEPRINT_NOT_FOUND", "Blueprint not found"),
        )
    if bp.get("status") == "published":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=error_response("BLUEPRINT_PUBLISHED", "Cannot modify steps of a published blueprint"),
        )
    return bp


async def get_all_child_ids(db, step_id: str, blueprint_id: str) -> List[str]:
    """Get all descendant step IDs."""
    children_cursor = db["steps"].find({"blueprint_id": blueprint_id, "parent_id": step_id})
    children = await children_cursor.to_list(length=1000)
    ids = []
    for child in children:
        ids.append(child["_id"])
        ids.extend(await get_all_child_ids(db, child["_id"], blueprint_id))
    return ids


@router.get("/")
async def list_steps(
    blueprint_id: str,
    _: dict = Depends(get_current_user),
):
    db = get_db()
    bp = await db["blueprints"].find_one({"_id": blueprint_id})
    if not bp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("BLUEPRINT_NOT_FOUND", "Blueprint not found"),
        )

    tree = await get_step_tree(db, blueprint_id)
    return success_response(tree)


@router.post("/", status_code=status.HTTP_201_CREATED)
async def add_step(
    blueprint_id: str,
    body: CreateStepRequest,
    current_user: dict = Depends(require_roles("admin", "editor")),
):
    db = get_db()
    await check_blueprint_published(db, blueprint_id)

    if body.type not in VALID_STEP_TYPES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=error_response("VALIDATION_ERROR", f"Step type must be one of: {', '.join(VALID_STEP_TYPES)}"),
        )

    if body.on_failure not in VALID_ON_FAILURE:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=error_response("VALIDATION_ERROR", f"on_failure must be one of: {', '.join(VALID_ON_FAILURE)}"),
        )

    if body.parent_id:
        parent = await db["steps"].find_one({"_id": body.parent_id, "blueprint_id": blueprint_id})
        if not parent:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=error_response("PARENT_STEP_NOT_FOUND", "Parent step not found"),
            )

    if body.script_id:
        script = await db["scripts"].find_one({"_id": body.script_id})
        if not script:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=error_response("SCRIPT_NOT_FOUND", "Script not found"),
            )

    now = datetime.now(timezone.utc).isoformat()
    step_id = str(uuid.uuid4())
    step_doc = {
        "_id": step_id,
        "blueprint_id": blueprint_id,
        "name": body.name,
        "type": body.type,
        "parent_id": body.parent_id,
        "order": body.order,
        "script_id": body.script_id,
        "script_params": body.script_params,
        "entry": body.entry,
        "dependencies": body.dependencies,
        "on_failure": body.on_failure,
        "retry_count": body.retry_count,
        "timeout_seconds": body.timeout_seconds,
        "validation_rules": body.validation_rules,
        "created_at": now,
    }

    await db["steps"].insert_one(step_doc)
    return success_response(doc_to_dict(step_doc))


@router.get("/{step_id}")
async def get_step(
    blueprint_id: str,
    step_id: str,
    _: dict = Depends(get_current_user),
):
    db = get_db()
    bp = await db["blueprints"].find_one({"_id": blueprint_id})
    if not bp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("BLUEPRINT_NOT_FOUND", "Blueprint not found"),
        )

    step = await db["steps"].find_one({"_id": step_id, "blueprint_id": blueprint_id})
    if not step:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("STEP_NOT_FOUND", "Step not found"),
        )

    step_dict = doc_to_dict(step)
    step_dict["children"] = await get_step_tree(db, blueprint_id, step_id)
    return success_response(step_dict)


@router.put("/{step_id}")
async def update_step(
    blueprint_id: str,
    step_id: str,
    body: UpdateStepRequest,
    current_user: dict = Depends(require_roles("admin", "editor")),
):
    db = get_db()
    await check_blueprint_published(db, blueprint_id)

    step = await db["steps"].find_one({"_id": step_id, "blueprint_id": blueprint_id})
    if not step:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("STEP_NOT_FOUND", "Step not found"),
        )

    if body.script_id:
        script = await db["scripts"].find_one({"_id": body.script_id})
        if not script:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=error_response("SCRIPT_NOT_FOUND", "Script not found"),
            )

    updates = {}
    for field in ["name", "type", "script_id", "script_params", "entry",
                  "dependencies", "on_failure", "retry_count", "timeout_seconds", "validation_rules"]:
        val = getattr(body, field)
        if val is not None:
            updates[field] = val

    if updates:
        await db["steps"].update_one({"_id": step_id}, {"$set": updates})

    updated = await db["steps"].find_one({"_id": step_id})
    return success_response(doc_to_dict(updated))


@router.delete("/{step_id}")
async def delete_step(
    blueprint_id: str,
    step_id: str,
    current_user: dict = Depends(require_roles("admin", "editor")),
):
    db = get_db()
    await check_blueprint_published(db, blueprint_id)

    step = await db["steps"].find_one({"_id": step_id, "blueprint_id": blueprint_id})
    if not step:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("STEP_NOT_FOUND", "Step not found"),
        )

    # Check if other steps depend on this one
    dependent = await db["steps"].find_one({
        "blueprint_id": blueprint_id,
        "dependencies": step_id,
        "_id": {"$ne": step_id},
    })
    if dependent:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=error_response("HAS_DEPENDENTS", "Cannot delete step that other steps depend on"),
        )

    # Delete step and all children
    child_ids = await get_all_child_ids(db, step_id, blueprint_id)
    all_ids = [step_id] + child_ids

    await db["steps"].delete_many({"_id": {"$in": all_ids}})
    return success_response({"message": "Step deleted successfully", "deleted_count": len(all_ids)})


@router.put("/reorder")
async def reorder_steps(
    blueprint_id: str,
    body: ReorderRequest,
    current_user: dict = Depends(require_roles("admin", "editor")),
):
    db = get_db()
    await check_blueprint_published(db, blueprint_id)

    bp = await db["blueprints"].find_one({"_id": blueprint_id})
    if not bp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("BLUEPRINT_NOT_FOUND", "Blueprint not found"),
        )

    for idx, step_id in enumerate(body.step_ids):
        await db["steps"].update_one(
            {"_id": step_id, "blueprint_id": blueprint_id},
            {"$set": {"order": idx}},
        )

    return success_response({"message": "Steps reordered successfully"})
