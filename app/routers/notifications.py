from fastapi import APIRouter, HTTPException, status, Depends
from datetime import datetime, timezone

from app.database import get_db, doc_to_dict, docs_to_list
from app.dependencies import get_current_user, require_roles, CommonQueryParams
from app.response import success_response, error_response, paginate, paginate_meta

router = APIRouter(prefix="/v1/notifications", tags=["notifications"])


def _with_read(doc: dict, user_id: str) -> dict:
    """Add per-user read flag to a notification dict."""
    d = doc_to_dict(doc)
    read_by_ids = {entry["user_id"] for entry in (d.get("read_by") or [])}
    d["read"] = user_id in read_by_ids
    return d


@router.get("/")
async def list_notifications(
    commons: CommonQueryParams = Depends(),
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    docs, total = await paginate(
        db["notifications"], {}, commons.page, commons.limit,
        "created_at", -1,
    )
    user_id = current_user["id"]
    notifications = [_with_read(d, user_id) for d in docs]
    return success_response(notifications, paginate_meta(commons.page, commons.limit, total))


@router.get("/unread-count")
async def unread_count(current_user: dict = Depends(get_current_user)):
    db = get_db()
    user_id = current_user["id"]
    count = await db["notifications"].count_documents({
        "read_by.user_id": {"$ne": user_id},
    })
    return success_response({"unread_count": count})


@router.put("/read-all")
async def mark_all_read(current_user: dict = Depends(get_current_user)):
    db = get_db()
    user_id = current_user["id"]
    now = datetime.now(timezone.utc).isoformat()
    result = await db["notifications"].update_many(
        {"read_by.user_id": {"$ne": user_id}},
        {"$push": {"read_by": {"user_id": user_id, "read_at": now}}},
    )
    return success_response({"updated_count": result.modified_count})


@router.put("/{notification_id}/read")
async def mark_read(
    notification_id: str,
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    user_id = current_user["id"]
    notification = await db["notifications"].find_one({"_id": notification_id})
    if not notification:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("NOTIFICATION_NOT_FOUND", "Notification not found"),
        )

    read_by_ids = {entry["user_id"] for entry in (notification.get("read_by") or [])}
    if user_id not in read_by_ids:
        now = datetime.now(timezone.utc).isoformat()
        await db["notifications"].update_one(
            {"_id": notification_id},
            {"$push": {"read_by": {"user_id": user_id, "read_at": now}}},
        )

    updated = await db["notifications"].find_one({"_id": notification_id})
    return success_response(_with_read(updated, user_id))


@router.delete("/{notification_id}")
async def delete_notification(
    notification_id: str,
    _: dict = Depends(require_roles("admin")),
):
    db = get_db()
    notification = await db["notifications"].find_one({"_id": notification_id})
    if not notification:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("NOTIFICATION_NOT_FOUND", "Notification not found"),
        )

    await db["notifications"].delete_one({"_id": notification_id})
    return success_response({"message": "Notification deleted successfully"})
