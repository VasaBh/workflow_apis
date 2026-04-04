from fastapi import APIRouter, HTTPException, status, Depends
from datetime import datetime, timezone

from app.database import get_db, doc_to_dict, docs_to_list
from app.dependencies import get_current_user, CommonQueryParams
from app.response import success_response, error_response, paginate, paginate_meta

router = APIRouter(prefix="/v1/notifications", tags=["notifications"])


@router.get("/")
async def list_notifications(
    commons: CommonQueryParams = Depends(),
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    query = {"user_id": current_user["id"]}
    docs, total = await paginate(
        db["notifications"], query, commons.page, commons.limit,
        "created_at", -1,
    )
    return success_response(docs_to_list(docs), paginate_meta(commons.page, commons.limit, total))


@router.get("/unread-count")
async def unread_count(current_user: dict = Depends(get_current_user)):
    db = get_db()
    count = await db["notifications"].count_documents({
        "user_id": current_user["id"],
        "read": False,
    })
    return success_response({"unread_count": count})


@router.put("/read-all")
async def mark_all_read(current_user: dict = Depends(get_current_user)):
    db = get_db()
    result = await db["notifications"].update_many(
        {"user_id": current_user["id"], "read": False},
        {"$set": {"read": True}},
    )
    return success_response({"updated_count": result.modified_count})


@router.put("/{notification_id}/read")
async def mark_read(
    notification_id: str,
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    notification = await db["notifications"].find_one({
        "_id": notification_id,
        "user_id": current_user["id"],
    })
    if not notification:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("NOTIFICATION_NOT_FOUND", "Notification not found"),
        )

    await db["notifications"].update_one(
        {"_id": notification_id},
        {"$set": {"read": True}},
    )
    updated = await db["notifications"].find_one({"_id": notification_id})
    return success_response(doc_to_dict(updated))


@router.delete("/{notification_id}")
async def delete_notification(
    notification_id: str,
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    notification = await db["notifications"].find_one({
        "_id": notification_id,
        "user_id": current_user["id"],
    })
    if not notification:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_response("NOTIFICATION_NOT_FOUND", "Notification not found"),
        )

    await db["notifications"].delete_one({"_id": notification_id})
    return success_response({"message": "Notification deleted successfully"})
