from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel, field_validator
from datetime import datetime, timezone
from typing import Optional, List
import uuid
import re

from app.database import get_db, doc_to_dict, docs_to_list
from app.dependencies import get_current_user, require_roles, CommonQueryParams
from app.response import success_response, error_response, paginate, paginate_meta
from app.sandbox import execute_script

router = APIRouter(prefix="/v1/scripts", tags=["scripts"])

VALID_PARAM_TYPES = ["string", "integer", "float", "boolean", "object", "array"]
ENTRY_REGEX = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class ScriptParameter(BaseModel):
    name: str
    type: str
    description: Optional[str] = None
    required: bool = True
    default: Optional[str] = None

    @field_validator("type")
    @classmethod
    def validate_type(cls, v):
        if v not in VALID_PARAM_TYPES:
            raise ValueError(f"Parameter type must be one of: {', '.join(VALID_PARAM_TYPES)}")
        return v


class CreateScriptRequest(BaseModel):
    name: str
    description: Optional[str] = None
    language: str = "python"
    entry: str
    code: str
    parameters: List[ScriptParameter] = []

    @field_validator("language")
    @classmethod
    def validate_language(cls, v):
        if v != "python":
            raise ValueError("Only Python language is supported")
        return v

    @field_validator("entry")
    @classmethod
    def validate_entry(cls, v):
        if not ENTRY_REGEX.match(v):
            raise ValueError("Entry must be a valid Python function name")
        return v

    @field_validator("code")
    @classmethod
    def validate_code(cls, v):
        if not v or not v.strip():
            raise ValueError("Code cannot be empty")
        return v

    @field_validator("parameters")
    @classmethod
    def validate_param_names_unique(cls, v):
        names = [p.name for p in v]
        if len(names) != len(set(names)):
            raise ValueError("Parameter names must be unique within a script")
        return v


class UpdateScriptRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    entry: Optional[str] = None
    code: Optional[str] = None
    parameters: Optional[List[ScriptParameter]] = None

    @field_validator("entry")
    @classmethod
    def validate_entry(cls, v):
        if v is not None and not ENTRY_REGEX.match(v):
            raise ValueError("Entry must be a valid Python function name")
        return v

    @field_validator("code")
    @classmethod
    def validate_code(cls, v):
        if v is not None and not v.strip():
            raise ValueError("Code cannot be empty")
        return v

    @field_validator("parameters")
    @classmethod
    def validate_param_names_unique(cls, v):
        if v is not None:
            names = [p.name for p in v]
            if len(names) != len(set(names)):
                raise ValueError("Parameter names must be unique within a script")
        return v


class ValidateCodeRequest(BaseModel):
    code: str
    entry: str
    parameters: List[ScriptParameter] = []
    test_params: dict = {}

    @field_validator("entry")
    @classmethod
    def validate_entry(cls, v):
        if not ENTRY_REGEX.match(v):
            raise ValueError("Entry must be a valid Python function name")
        return v


class CloneScriptRequest(BaseModel):
    name: str


class RestoreVersionRequest(BaseModel):
    pass


async def check_script_in_active_run(db, script_id: str) -> bool:
    """Check if script is used in any active run via step_runs."""
    active_run = await db["runs"].find_one({
        "status": {"$in": ["in_progress", "paused"]}
    })
    if not active_run:
        return False

    # Check if any step in an active run uses this script
    step_run = await db["step_runs"].find_one({
        "run_id": active_run["_id"],
        "script_id": script_id,
        "status": {"$in": ["in_progress", "not_started", "paused"]},
    })
    return step_run is not None


async def check_script_attached_to_blueprint(db, script_id: str) -> bool:
    """Check if script is referenced by any blueprint step."""
    step = await db["steps"].find_one({"script_id": script_id})
    return step is not None


@router.get("/")
async def list_scripts(
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
        db["scripts"], query, commons.page, commons.limit,
        commons.sort, commons.sort_direction
    )
    scripts = docs_to_list(docs)
    # Don't return full code in list
    for s in scripts:
        s.pop("code", None)

    return success_response(scripts, paginate_meta(commons.page, commons.limit, total))


@router.post("/validate")
async def validate_code(
    body: ValidateCodeRequest,
    _: dict = Depends(require_roles("admin", "editor")),
):
    result = await execute_script(body.code, body.entry, body.test_params)
    if not result["valid"] and result.get("error", "").startswith("Execution timed out"):
        raise HTTPException(
            status_code=status.HTTP_408_REQUEST_TIMEOUT,
            detail=error_response("EXECUTION_TIMEOUT", result["error"]),
        )
    return success_response(result)


@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_script(
    body: CreateScriptRequest,
    current_user: dict = Depends(require_roles("admin", "editor")),
):
    db = get_db()

    existing = await db["scripts"].find_one({"name": body.name})
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=error_response("NAME_ALREADY_EXISTS", "A script with this name already exists"),
        )

    now = datetime.now(timezone.utc).isoformat()
    script_id = str(uuid.uuid4())
    params = [p.model_dump() for p in body.parameters]
    script_doc = {
        "_id": script_id,
        "name": body.name,
        "description": body.description,
        "language": body.language,
        "entry": body.entry,
        "code": body.code,
        "parameters": params,
        "version": 1,
        "created_at": now,
        "updated_at": now,
        "created_by": current_user["id"],
    }

    await db["scripts"].insert_one(script_doc)
    return success_response(doc_to_dict(script_doc))


@router.get("/{script_id}")
async def get_script(
    script_id: str,
    _: dict = Depends(get_current_user),
):
    db = get_db()
    script = await db["scripts"].find_one({"_id": script_id})
    if not script:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("SCRIPT_NOT_FOUND", "Script not found"),
        )
    return success_response(doc_to_dict(script))


@router.put("/{script_id}")
async def update_script(
    script_id: str,
    body: UpdateScriptRequest,
    current_user: dict = Depends(require_roles("admin", "editor")),
):
    db = get_db()
    script = await db["scripts"].find_one({"_id": script_id})
    if not script:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("SCRIPT_NOT_FOUND", "Script not found"),
        )

    if await check_script_in_active_run(db, script_id):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=error_response("SCRIPT_IN_USE", "Cannot update script while it is in an active run"),
        )

    if body.name is not None:
        existing = await db["scripts"].find_one({"name": body.name, "_id": {"$ne": script_id}})
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=error_response("NAME_ALREADY_EXISTS", "A script with this name already exists"),
            )

    # Save version to history before update
    version_doc = {
        "_id": str(uuid.uuid4()),
        "script_id": script_id,
        "version": script["version"],
        "code": script["code"],
        "entry": script["entry"],
        "parameters": script.get("parameters", []),
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    await db["script_versions"].insert_one(version_doc)

    now = datetime.now(timezone.utc).isoformat()
    updates = {
        "updated_at": now,
        "version": script["version"] + 1,
    }

    if body.name is not None:
        updates["name"] = body.name
    if body.description is not None:
        updates["description"] = body.description
    if body.entry is not None:
        updates["entry"] = body.entry
    if body.code is not None:
        updates["code"] = body.code
    if body.parameters is not None:
        updates["parameters"] = [p.model_dump() for p in body.parameters]

    await db["scripts"].update_one({"_id": script_id}, {"$set": updates})
    updated = await db["scripts"].find_one({"_id": script_id})
    return success_response(doc_to_dict(updated))


@router.delete("/{script_id}")
async def delete_script(
    script_id: str,
    _: dict = Depends(require_roles("admin")),
):
    db = get_db()
    script = await db["scripts"].find_one({"_id": script_id})
    if not script:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("SCRIPT_NOT_FOUND", "Script not found"),
        )

    if await check_script_attached_to_blueprint(db, script_id):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=error_response("SCRIPT_IN_USE", "Cannot delete script that is attached to a blueprint step"),
        )

    await db["scripts"].delete_one({"_id": script_id})
    await db["script_versions"].delete_many({"script_id": script_id})
    return success_response({"message": "Script deleted successfully"})


class ValidateSavedScriptRequest(BaseModel):
    test_params: dict = {}


@router.post("/{script_id}/validate")
async def validate_saved_script(
    script_id: str,
    body: ValidateSavedScriptRequest = None,
    _: dict = Depends(require_roles("admin", "editor")),
):
    db = get_db()
    script = await db["scripts"].find_one({"_id": script_id})
    if not script:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("SCRIPT_NOT_FOUND", "Script not found"),
        )

    test_params = body.test_params if body else {}
    result = await execute_script(script["code"], script["entry"], test_params)
    if not result["valid"] and result.get("error", "").startswith("Execution timed out"):
        raise HTTPException(
            status_code=status.HTTP_408_REQUEST_TIMEOUT,
            detail=error_response("EXECUTION_TIMEOUT", result["error"]),
        )
    return success_response(result)


@router.post("/{script_id}/clone")
async def clone_script(
    script_id: str,
    body: CloneScriptRequest,
    current_user: dict = Depends(require_roles("admin", "editor")),
):
    db = get_db()
    script = await db["scripts"].find_one({"_id": script_id})
    if not script:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("SCRIPT_NOT_FOUND", "Script not found"),
        )

    existing = await db["scripts"].find_one({"name": body.name})
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=error_response("NAME_ALREADY_EXISTS", "A script with this name already exists"),
        )

    now = datetime.now(timezone.utc).isoformat()
    new_id = str(uuid.uuid4())
    new_doc = {
        "_id": new_id,
        "name": body.name,
        "description": script.get("description"),
        "language": script["language"],
        "entry": script["entry"],
        "code": script["code"],
        "parameters": script.get("parameters", []),
        "version": 1,
        "created_at": now,
        "updated_at": now,
        "created_by": current_user["id"],
    }

    await db["scripts"].insert_one(new_doc)
    return success_response(doc_to_dict(new_doc))


@router.get("/{script_id}/versions")
async def get_script_versions(
    script_id: str,
    commons: CommonQueryParams = Depends(),
    _: dict = Depends(get_current_user),
):
    db = get_db()
    script = await db["scripts"].find_one({"_id": script_id})
    if not script:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("SCRIPT_NOT_FOUND", "Script not found"),
        )

    docs, total = await paginate(
        db["script_versions"],
        {"script_id": script_id},
        commons.page, commons.limit,
        "saved_at", -1
    )
    return success_response(docs_to_list(docs), paginate_meta(commons.page, commons.limit, total))


@router.post("/{script_id}/restore/{version}")
async def restore_script_version(
    script_id: str,
    version: int,
    current_user: dict = Depends(require_roles("admin", "editor")),
):
    db = get_db()
    script = await db["scripts"].find_one({"_id": script_id})
    if not script:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("SCRIPT_NOT_FOUND", "Script not found"),
        )

    version_doc = await db["script_versions"].find_one({
        "script_id": script_id,
        "version": version,
    })
    if not version_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("SCRIPT_NOT_FOUND", f"Version {version} not found for this script"),
        )

    # Save current version to history first
    current_version_doc = {
        "_id": str(uuid.uuid4()),
        "script_id": script_id,
        "version": script["version"],
        "code": script["code"],
        "entry": script["entry"],
        "parameters": script.get("parameters", []),
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    await db["script_versions"].insert_one(current_version_doc)

    now = datetime.now(timezone.utc).isoformat()
    updates = {
        "code": version_doc["code"],
        "entry": version_doc["entry"],
        "parameters": version_doc.get("parameters", []),
        "version": script["version"] + 1,
        "updated_at": now,
    }

    await db["scripts"].update_one({"_id": script_id}, {"$set": updates})
    updated = await db["scripts"].find_one({"_id": script_id})
    return success_response(doc_to_dict(updated))
