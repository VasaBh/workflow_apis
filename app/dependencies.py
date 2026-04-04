from fastapi import Depends, HTTPException, status, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, ExpiredSignatureError, jwt
from app.config import settings
from app.database import get_db, doc_to_dict
from dataclasses import dataclass, field
from typing import List, Optional
import pymongo

security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> dict:
    token = credentials.credentials
    db = get_db()

    # Check if token is blacklisted
    blacklisted = await db["token_blacklist"].find_one({"token": token})
    if blacklisted:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "TOKEN_INVALID",
                "message": "Token has been revoked",
                "details": {},
            },
        )

    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET,
            algorithms=[settings.JWT_ALGORITHM],
        )
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "TOKEN_EXPIRED",
                "message": "Token has expired",
                "details": {},
            },
        )
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "TOKEN_INVALID",
                "message": "Invalid token",
                "details": {},
            },
        )

    token_type = payload.get("type")
    if token_type != "access":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "TOKEN_INVALID",
                "message": "Invalid token type",
                "details": {},
            },
        )

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "TOKEN_INVALID",
                "message": "Invalid token payload",
                "details": {},
            },
        )

    user = await db["users"].find_one({"_id": user_id})
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "TOKEN_INVALID",
                "message": "User not found",
                "details": {},
            },
        )

    return doc_to_dict(user)


def require_roles(*roles: str):
    """Return a dependency that checks user role."""

    async def role_checker(
        current_user: dict = Depends(get_current_user),
    ) -> dict:
        if current_user.get("role") not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "code": "FORBIDDEN",
                    "message": f"Required role(s): {', '.join(roles)}",
                    "details": {},
                },
            )
        return current_user

    return role_checker


@dataclass
class CommonQueryParams:
    page: int = Query(default=1, ge=1)
    limit: int = Query(default=20, ge=1, le=100)
    sort: str = Query(default="created_at")
    order: str = Query(default="desc", pattern="^(asc|desc)$")
    search: Optional[str] = Query(default=None)

    @property
    def sort_direction(self) -> int:
        return pymongo.ASCENDING if self.order == "asc" else pymongo.DESCENDING
