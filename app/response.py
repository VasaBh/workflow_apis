from typing import Any, Optional
from pydantic import BaseModel


class MetaModel(BaseModel):
    page: int = 1
    limit: int = 20
    total: int = 0


class ErrorDetail(BaseModel):
    code: str
    message: str
    details: dict = {}


class APIResponse(BaseModel):
    success: bool
    data: Any = None
    error: Optional[ErrorDetail] = None
    meta: Optional[MetaModel] = None


def success_response(data: Any = None, meta: Optional[dict] = None) -> dict:
    response = {
        "success": True,
        "data": data,
        "error": None,
        "meta": meta,
    }
    return response


def error_response(code: str, message: str, details: dict = {}) -> dict:
    return {
        "success": False,
        "data": None,
        "error": {
            "code": code,
            "message": message,
            "details": details,
        },
        "meta": None,
    }


def paginate_meta(page: int, limit: int, total: int) -> dict:
    return {
        "page": page,
        "limit": limit,
        "total": total,
    }


async def paginate(collection, query: dict, page: int, limit: int, sort_field: str = "created_at", sort_order: int = -1) -> tuple:
    """Returns (documents list, total count)."""
    total = await collection.count_documents(query)
    skip = (page - 1) * limit
    cursor = collection.find(query).sort(sort_field, sort_order).skip(skip).limit(limit)
    docs = await cursor.to_list(length=limit)
    return docs, total
