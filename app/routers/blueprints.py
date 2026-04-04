from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel
from datetime import datetime, timezone
from typing import Optional, List
import uuid

from app.database import get_db, doc_to_dict, docs_to_list
from app.dependencies import get_current_user, require_roles, CommonQueryParams
from app.response import success_response, error_response, paginate, paginate_meta

router = APIRouter(prefix="/v1/blueprints", tags=["blueprints"])


class CreateBlueprintRequest(BaseModel):
    name: str
    description: Optional[str] = None


class UpdateBlueprintRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None


class CloneBlueprintRequest(BaseModel):
    name: str


async def get_step_tree(db, blueprint_id: str) -> List[dict]:
    """Build a tree of steps for a blueprint."""
    steps_cursor = db["steps"].find({"blueprint_id": blueprint_id}).sort("order", 1)
    steps = await steps_cursor.to_list(length=1000)
    steps = docs_to_list(steps)

    step_map = {s["id"]: s for s in steps}
    roots = []

    for step in steps:
        step["children"] = []

    for step in steps:
        parent_id = step.get("parent_id")
        if parent_id and parent_id in step_map:
            step_map[parent_id]["children"].append(step)
        else:
            roots.append(step)

    return roots


async def has_circular_dependency(db, blueprint_id: str) -> bool:
    """Check for circular dependencies within blueprint steps."""
    steps_cursor = db["steps"].find({"blueprint_id": blueprint_id})
    steps = await steps_cursor.to_list(length=1000)

    step_ids = {s["_id"] for s in steps}
    adj = {s["_id"]: s.get("dependencies", []) for s in steps}

    visited = set()
    rec_stack = set()

    def dfs(node):
        visited.add(node)
        rec_stack.add(node)
        for neighbor in adj.get(node, []):
            if neighbor not in step_ids:
                continue
            if neighbor not in visited:
                if dfs(neighbor):
                    return True
            elif neighbor in rec_stack:
                return True
        rec_stack.discard(node)
        return False

    for step_id in step_ids:
        if step_id not in visited:
            if dfs(step_id):
                return True
    return False


@router.get("/")
async def list_blueprints(
    commons: CommonQueryParams = Depends(),
    _: dict = Depends(get_current_user),
):
    db = get_db()
    query = {}
    if commons.search:
        query["$or"] = [
            {"name": {"$regex": commons.search, "$options": "i"}},
            {"description": {"$regex": commons.search, "$options": "i"}},
        ]

    docs, total = await paginate(
        db["blueprints"], query, commons.page, commons.limit,
        commons.sort, commons.sort_direction
    )
    return success_response(docs_to_list(docs), paginate_meta(commons.page, commons.limit, total))


@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_blueprint(
    body: CreateBlueprintRequest,
    current_user: dict = Depends(require_roles("admin", "editor")),
):
    db = get_db()
    existing = await db["blueprints"].find_one({"name": body.name})
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=error_response("NAME_ALREADY_EXISTS", "A blueprint with this name already exists"),
        )

    now = datetime.now(timezone.utc).isoformat()
    bp_id = str(uuid.uuid4())
    doc = {
        "_id": bp_id,
        "name": body.name,
        "description": body.description,
        "status": "draft",
        "version": 1,
        "created_at": now,
        "updated_at": now,
        "published_at": None,
        "created_by": current_user["id"],
    }

    await db["blueprints"].insert_one(doc)
    return success_response(doc_to_dict(doc))


@router.get("/{blueprint_id}")
async def get_blueprint(
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

    bp_dict = doc_to_dict(bp)
    bp_dict["steps"] = await get_step_tree(db, blueprint_id)
    return success_response(bp_dict)


@router.put("/{blueprint_id}")
async def update_blueprint(
    blueprint_id: str,
    body: UpdateBlueprintRequest,
    current_user: dict = Depends(require_roles("admin", "editor")),
):
    db = get_db()
    bp = await db["blueprints"].find_one({"_id": blueprint_id})
    if not bp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("BLUEPRINT_NOT_FOUND", "Blueprint not found"),
        )

    if bp.get("status") == "published":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=error_response("BLUEPRINT_PUBLISHED", "Cannot update a published blueprint"),
        )

    updates = {"updated_at": datetime.now(timezone.utc).isoformat()}
    if body.name is not None:
        existing = await db["blueprints"].find_one({"name": body.name, "_id": {"$ne": blueprint_id}})
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=error_response("NAME_ALREADY_EXISTS", "A blueprint with this name already exists"),
            )
        updates["name"] = body.name
    if body.description is not None:
        updates["description"] = body.description

    await db["blueprints"].update_one({"_id": blueprint_id}, {"$set": updates})
    updated = await db["blueprints"].find_one({"_id": blueprint_id})
    return success_response(doc_to_dict(updated))


@router.delete("/{blueprint_id}")
async def delete_blueprint(
    blueprint_id: str,
    _: dict = Depends(require_roles("admin")),
):
    db = get_db()
    bp = await db["blueprints"].find_one({"_id": blueprint_id})
    if not bp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("BLUEPRINT_NOT_FOUND", "Blueprint not found"),
        )

    active_run = await db["runs"].find_one({
        "blueprint_id": blueprint_id,
        "status": {"$in": ["in_progress", "paused"]},
    })
    if active_run:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=error_response("HAS_ACTIVE_RUNS", "Cannot delete blueprint with active runs"),
        )

    await db["blueprints"].delete_one({"_id": blueprint_id})
    await db["steps"].delete_many({"blueprint_id": blueprint_id})
    return success_response({"message": "Blueprint deleted successfully"})


@router.post("/{blueprint_id}/publish")
async def publish_blueprint(
    blueprint_id: str,
    current_user: dict = Depends(require_roles("admin", "editor")),
):
    db = get_db()
    bp = await db["blueprints"].find_one({"_id": blueprint_id})
    if not bp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("BLUEPRINT_NOT_FOUND", "Blueprint not found"),
        )

    if bp.get("status") == "published":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=error_response("BLUEPRINT_PUBLISHED", "Blueprint is already published"),
        )

    # Check has at least one step
    step_count = await db["steps"].count_documents({"blueprint_id": blueprint_id})
    if step_count == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("NO_STEPS", "Blueprint must have at least one step to publish"),
        )

    # Validate all script steps have valid script_id
    script_steps_cursor = db["steps"].find({"blueprint_id": blueprint_id, "type": "script"})
    script_steps = await script_steps_cursor.to_list(length=1000)
    for step in script_steps:
        script_id = step.get("script_id")
        if not script_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_response("NO_STEPS", f"Script step '{step.get('name')}' has no script assigned"),
            )
        script = await db["scripts"].find_one({"_id": script_id})
        if not script:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_response("SCRIPT_NOT_FOUND", f"Script step '{step.get('name')}' references a missing script"),
            )

    # Check for circular dependencies
    if await has_circular_dependency(db, blueprint_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("CIRCULAR_DEPENDENCY", "Blueprint has circular dependencies between steps"),
        )

    now = datetime.now(timezone.utc).isoformat()
    await db["blueprints"].update_one(
        {"_id": blueprint_id},
        {"$set": {"status": "published", "published_at": now, "updated_at": now}},
    )

    # Save to version history
    updated = await db["blueprints"].find_one({"_id": blueprint_id})
    version_doc = {
        "_id": str(uuid.uuid4()),
        "blueprint_id": blueprint_id,
        "version": updated["version"],
        "name": updated["name"],
        "description": updated.get("description"),
        "status": "published",
        "published_at": now,
        "saved_at": now,
    }
    await db["blueprint_versions"].insert_one(version_doc)

    return success_response(doc_to_dict(updated))


@router.post("/{blueprint_id}/clone")
async def clone_blueprint(
    blueprint_id: str,
    body: CloneBlueprintRequest,
    current_user: dict = Depends(require_roles("admin", "editor")),
):
    db = get_db()
    bp = await db["blueprints"].find_one({"_id": blueprint_id})
    if not bp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("BLUEPRINT_NOT_FOUND", "Blueprint not found"),
        )

    existing = await db["blueprints"].find_one({"name": body.name})
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=error_response("NAME_ALREADY_EXISTS", "A blueprint with this name already exists"),
        )

    now = datetime.now(timezone.utc).isoformat()
    new_bp_id = str(uuid.uuid4())
    new_bp = {
        "_id": new_bp_id,
        "name": body.name,
        "description": bp.get("description"),
        "status": "draft",
        "version": 1,
        "created_at": now,
        "updated_at": now,
        "published_at": None,
        "created_by": current_user["id"],
    }
    await db["blueprints"].insert_one(new_bp)

    # Clone steps
    steps_cursor = db["steps"].find({"blueprint_id": blueprint_id})
    steps = await steps_cursor.to_list(length=1000)
    old_to_new_id = {}
    for step in steps:
        old_id = step["_id"]
        new_step_id = str(uuid.uuid4())
        old_to_new_id[old_id] = new_step_id

    for step in steps:
        new_step = dict(step)
        old_id = new_step.pop("_id")
        new_step["_id"] = old_to_new_id[old_id]
        new_step["blueprint_id"] = new_bp_id
        new_step["created_at"] = now

        if new_step.get("parent_id") and new_step["parent_id"] in old_to_new_id:
            new_step["parent_id"] = old_to_new_id[new_step["parent_id"]]

        if new_step.get("dependencies"):
            new_step["dependencies"] = [
                old_to_new_id.get(dep, dep) for dep in new_step["dependencies"]
            ]

        await db["steps"].insert_one(new_step)

    return success_response(doc_to_dict(new_bp))


@router.get("/{blueprint_id}/versions")
async def get_blueprint_versions(
    blueprint_id: str,
    commons: CommonQueryParams = Depends(),
    _: dict = Depends(get_current_user),
):
    db = get_db()
    bp = await db["blueprints"].find_one({"_id": blueprint_id})
    if not bp:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("BLUEPRINT_NOT_FOUND", "Blueprint not found"),
        )

    docs, total = await paginate(
        db["blueprint_versions"],
        {"blueprint_id": blueprint_id},
        commons.page, commons.limit,
        "saved_at", -1
    )
    return success_response(docs_to_list(docs), paginate_meta(commons.page, commons.limit, total))
