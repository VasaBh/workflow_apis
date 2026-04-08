from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from jose import JWTError, ExpiredSignatureError, jwt
import bcrypt as _bcrypt
from datetime import datetime, timedelta, timezone
from app.config import settings
from app.database import get_db, doc_to_dict
from app.response import success_response, error_response
from app.dependencies import get_current_user

router = APIRouter(prefix="/v1/auth", tags=["auth"])
security = HTTPBearer()


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


def create_token(user_id: str, role: str, token_type: str, expires_delta: timedelta) -> str:
    expire = datetime.now(timezone.utc) + expires_delta
    payload = {
        "sub": user_id,
        "role": role,
        "type": token_type,
        "exp": expire,
    }
    return jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return _bcrypt.checkpw(plain_password.encode(), hashed_password.encode())


@router.post("/login")
async def login(body: LoginRequest):
    db = get_db()
    user = await db["users"].find_one({"email": body.email})

    if not user or not verify_password(body.password, user.get("password_hash", "")):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=error_response("INVALID_CREDENTIALS", "Invalid email or password"),
        )

    now = datetime.now(timezone.utc).isoformat()
    await db["users"].update_one({"_id": user["_id"]}, {"$set": {"last_login": now}})

    user_dict = doc_to_dict(user)
    user_dict["last_login"] = now
    access_token = create_token(
        user_dict["id"],
        user_dict["role"],
        "access",
        timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
    )
    refresh_token = create_token(
        user_dict["id"],
        user_dict["role"],
        "refresh",
        timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS),
    )

    # Remove sensitive fields from user response
    user_response = {k: v for k, v in user_dict.items() if k != "password_hash"}

    return success_response({
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "user": user_response,
    })


@router.post("/logout")
async def logout(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    token = credentials.credentials

    # Decode to get expiry
    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET,
            algorithms=[settings.JWT_ALGORITHM],
        )
        exp = payload.get("exp")
        expires_at = datetime.fromtimestamp(exp, tz=timezone.utc) if exp else (
            datetime.now(timezone.utc) + timedelta(hours=1)
        )
    except Exception:
        expires_at = datetime.now(timezone.utc) + timedelta(hours=1)

    try:
        await db["token_blacklist"].insert_one({
            "_id": token[:100],  # Use truncated token as id
            "token": token,
            "user_id": current_user["id"],
            "expires_at": expires_at,
            "blacklisted_at": datetime.now(timezone.utc),
        })
    except Exception:
        pass  # Already blacklisted

    return success_response({"message": "Logged out successfully"})


@router.post("/refresh")
async def refresh_token(body: RefreshRequest):
    db = get_db()

    try:
        payload = jwt.decode(
            body.refresh_token,
            settings.JWT_SECRET,
            algorithms=[settings.JWT_ALGORITHM],
        )
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=error_response("TOKEN_EXPIRED", "Refresh token has expired"),
        )
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=error_response("TOKEN_INVALID", "Invalid refresh token"),
        )

    if payload.get("type") != "refresh":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=error_response("TOKEN_INVALID", "Invalid token type"),
        )

    user_id = payload.get("sub")
    user = await db["users"].find_one({"_id": user_id})
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=error_response("TOKEN_INVALID", "User not found"),
        )

    user_dict = doc_to_dict(user)
    access_token = create_token(
        user_dict["id"],
        user_dict["role"],
        "access",
        timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
    )

    return success_response({
        "access_token": access_token,
        "token_type": "bearer",
    })


@router.get("/me")
async def get_me(current_user: dict = Depends(get_current_user)):
    user_response = {k: v for k, v in current_user.items() if k != "password_hash"}
    return success_response(user_response)
