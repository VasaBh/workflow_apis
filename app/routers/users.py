from fastapi import APIRouter, HTTPException, status, Depends, Query
from pydantic import BaseModel, EmailStr, field_validator
import bcrypt as _bcrypt
from datetime import datetime, timezone
from typing import Optional
import uuid
import re

from app.database import get_db, doc_to_dict, docs_to_list
from app.dependencies import get_current_user, require_roles, CommonQueryParams
from app.response import success_response, error_response, paginate, paginate_meta

router = APIRouter(prefix="/v1/users", tags=["users"])

VALID_ROLES = ["admin", "editor", "executor", "viewer"]


class CreateUserRequest(BaseModel):
    name: str
    email: EmailStr
    password: str
    role: str = "viewer"

    @field_validator("name")
    @classmethod
    def name_min_length(cls, v):
        if len(v.strip()) < 2:
            raise ValueError("Name must be at least 2 characters")
        return v.strip()

    @field_validator("password")
    @classmethod
    def password_min_length(cls, v):
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters")
        return v

    @field_validator("role")
    @classmethod
    def role_valid(cls, v):
        if v not in VALID_ROLES:
            raise ValueError(f"Role must be one of: {', '.join(VALID_ROLES)}")
        return v


class UpdateUserRequest(BaseModel):
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    password: Optional[str] = None

    @field_validator("name")
    @classmethod
    def name_min_length(cls, v):
        if v is not None and len(v.strip()) < 2:
            raise ValueError("Name must be at least 2 characters")
        return v.strip() if v else v

    @field_validator("password")
    @classmethod
    def password_min_length(cls, v):
        if v is not None and len(v) < 8:
            raise ValueError("Password must be at least 8 characters")
        return v


class UpdateRoleRequest(BaseModel):
    role: str

    @field_validator("role")
    @classmethod
    def role_valid(cls, v):
        if v not in VALID_ROLES:
            raise ValueError(f"Role must be one of: {', '.join(VALID_ROLES)}")
        return v


def hash_password(password: str) -> str:
    return _bcrypt.hashpw(password.encode(), _bcrypt.gensalt()).decode()


def user_safe(user: dict) -> dict:
    return {k: v for k, v in user.items() if k != "password_hash"}


@router.get("/")
async def list_users(
    commons: CommonQueryParams = Depends(),
    _: dict = Depends(require_roles("admin")),
):
    db = get_db()
    query = {}
    if commons.search:
        query["$or"] = [
            {"name": {"$regex": commons.search, "$options": "i"}},
            {"email": {"$regex": commons.search, "$options": "i"}},
        ]

    docs, total = await paginate(
        db["users"], query, commons.page, commons.limit,
        commons.sort, commons.sort_direction
    )
    users = [user_safe(doc_to_dict(d)) for d in docs]
    return success_response(users, paginate_meta(commons.page, commons.limit, total))


@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_user(
    body: CreateUserRequest,
    _: dict = Depends(require_roles("admin")),
):
    db = get_db()

    existing = await db["users"].find_one({"email": body.email})
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=error_response("EMAIL_ALREADY_EXISTS", "A user with this email already exists"),
        )

    now = datetime.now(timezone.utc).isoformat()
    user_id = str(uuid.uuid4())
    user_doc = {
        "_id": user_id,
        "name": body.name,
        "email": body.email,
        "password_hash": hash_password(body.password),
        "role": body.role,
        "created_at": now,
        "updated_at": now,
    }

    await db["users"].insert_one(user_doc)
    return success_response(user_safe(doc_to_dict(user_doc)))


@router.get("/{user_id}")
async def get_user(
    user_id: str,
    _: dict = Depends(require_roles("admin")),
):
    db = get_db()
    user = await db["users"].find_one({"_id": user_id})
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("USER_NOT_FOUND", "User not found"),
        )
    return success_response(user_safe(doc_to_dict(user)))


@router.put("/{user_id}")
async def update_user(
    user_id: str,
    body: UpdateUserRequest,
    current_user: dict = Depends(require_roles("admin")),
):
    db = get_db()
    user = await db["users"].find_one({"_id": user_id})
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("USER_NOT_FOUND", "User not found"),
        )

    updates = {"updated_at": datetime.now(timezone.utc).isoformat()}

    if body.email is not None:
        existing = await db["users"].find_one({"email": body.email, "_id": {"$ne": user_id}})
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=error_response("EMAIL_ALREADY_EXISTS", "A user with this email already exists"),
            )
        updates["email"] = body.email

    if body.name is not None:
        updates["name"] = body.name

    if body.password is not None:
        updates["password_hash"] = hash_password(body.password)

    await db["users"].update_one({"_id": user_id}, {"$set": updates})
    updated = await db["users"].find_one({"_id": user_id})
    return success_response(user_safe(doc_to_dict(updated)))


@router.delete("/{user_id}")
async def delete_user(
    user_id: str,
    current_user: dict = Depends(require_roles("admin")),
):
    if current_user["id"] == user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("CANNOT_DELETE_SELF", "You cannot delete your own account"),
        )

    db = get_db()
    user = await db["users"].find_one({"_id": user_id})
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("USER_NOT_FOUND", "User not found"),
        )

    await db["users"].delete_one({"_id": user_id})
    return success_response({"message": "User deleted successfully"})


@router.put("/{user_id}/role")
async def update_user_role(
    user_id: str,
    body: UpdateRoleRequest,
    current_user: dict = Depends(require_roles("admin")),
):
    if current_user["id"] == user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_response("CANNOT_CHANGE_OWN_ROLE", "You cannot change your own role"),
        )

    db = get_db()
    user = await db["users"].find_one({"_id": user_id})
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("USER_NOT_FOUND", "User not found"),
        )

    await db["users"].update_one(
        {"_id": user_id},
        {"$set": {"role": body.role, "updated_at": datetime.now(timezone.utc).isoformat()}},
    )
    updated = await db["users"].find_one({"_id": user_id})
    return success_response(user_safe(doc_to_dict(updated)))
