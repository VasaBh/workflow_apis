"""
File-system storage backend.

Each collection is a directory under FILES_DATA_DIR.
Each document is a JSON file named by a filesystem-safe encoding of its _id.

Implements the same async interface as Motor so all routers work unchanged:
  db["collection"].find_one(filter)
  db["collection"].find(filter).sort(f, d).skip(n).limit(n).to_list(length)
  db["collection"].insert_one(doc)
  db["collection"].update_one(filter, update)
  db["collection"].update_many(filter, update)
  db["collection"].delete_one(filter)
  db["collection"].delete_many(filter)
  db["collection"].count_documents(filter)
  db["collection"].create_index(...)   # no-op
"""

import json
import re
import base64
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


# Collections that auto-expire documents via their expires_at field
_TTL_COLLECTIONS = {"token_blacklist", "schedule_locks"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _id_to_filename(doc_id: Any) -> str:
    """Encode any _id value into a filesystem-safe filename (no extension)."""
    raw = str(doc_id)
    # base64url gives ~1.33x expansion and is always filesystem-safe
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def _load_json(path: Path) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def _save_json(path: Path, doc: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(doc, f, default=str, ensure_ascii=False)
    tmp.replace(path)  # atomic rename


def _is_expired(doc: dict) -> bool:
    expires_at = doc.get("expires_at")
    if expires_at is None:
        return False
    try:
        if isinstance(expires_at, str):
            dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        elif isinstance(expires_at, datetime):
            dt = expires_at
        else:
            return False
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) > dt
    except (ValueError, TypeError):
        return False


def _matches(doc: dict, filter_dict: dict) -> bool:
    """Evaluate a MongoDB-style filter dict against a document."""
    for key, value in filter_dict.items():
        if key == "$or":
            if not any(_matches(doc, sub) for sub in value):
                return False
            continue
        if key == "$and":
            if not all(_matches(doc, sub) for sub in value):
                return False
            continue

        field_val = doc.get(key)

        if isinstance(value, dict):
            for op, op_val in value.items():
                if op == "$options":
                    continue  # consumed alongside $regex
                if op == "$in":
                    if field_val not in op_val:
                        return False
                elif op == "$nin":
                    if field_val in op_val:
                        return False
                elif op == "$ne":
                    if field_val == op_val:
                        return False
                elif op == "$gte":
                    if field_val is None:
                        return False
                    try:
                        if field_val < op_val:
                            return False
                    except TypeError:
                        return False
                elif op == "$lte":
                    if field_val is None:
                        return False
                    try:
                        if field_val > op_val:
                            return False
                    except TypeError:
                        return False
                elif op == "$gt":
                    if field_val is None:
                        return False
                    try:
                        if field_val <= op_val:
                            return False
                    except TypeError:
                        return False
                elif op == "$lt":
                    if field_val is None:
                        return False
                    try:
                        if field_val >= op_val:
                            return False
                    except TypeError:
                        return False
                elif op == "$regex":
                    flags = re.IGNORECASE if "i" in value.get("$options", "") else 0
                    if field_val is None or not re.search(op_val, str(field_val), flags):
                        return False
        else:
            if field_val != value:
                return False

    return True


def _apply_update(doc: dict, update: dict) -> dict:
    result = dict(doc)
    if "$set" in update:
        result.update(update["$set"])
    else:
        # Treat plain dict as $set (non-standard but defensive)
        result.update({k: v for k, v in update.items() if not k.startswith("$")})
    return result


# ---------------------------------------------------------------------------
# Cursor
# ---------------------------------------------------------------------------

class FileCursor:
    """Chainable async cursor matching Motor's interface."""

    def __init__(self, docs: list):
        self._docs = docs
        self._sort_field: Optional[str] = None
        self._sort_dir: int = -1
        self._skip_n: int = 0
        self._limit_n: Optional[int] = None

    def sort(self, field, direction=-1) -> "FileCursor":
        self._sort_field = field
        self._sort_dir = direction
        return self

    def skip(self, n: int) -> "FileCursor":
        self._skip_n = n
        return self

    def limit(self, n: int) -> "FileCursor":
        self._limit_n = n
        return self

    async def to_list(self, length=None) -> list:
        docs = list(self._docs)

        if self._sort_field:
            reverse = self._sort_dir == -1
            docs.sort(
                key=lambda d: (
                    d.get(self._sort_field) is None,
                    d.get(self._sort_field) or "",
                ),
                reverse=reverse,
            )

        docs = docs[self._skip_n:]
        if self._limit_n is not None:
            docs = docs[: self._limit_n]
        if length is not None:
            docs = docs[:length]
        return docs


# ---------------------------------------------------------------------------
# Collection
# ---------------------------------------------------------------------------

class FileCollection:
    """Motor-compatible async collection backed by JSON files."""

    def __init__(self, base_path: Path, name: str):
        self._path = base_path / name
        self._path.mkdir(parents=True, exist_ok=True)
        self._is_ttl = name in _TTL_COLLECTIONS

    # -- internal helpers --------------------------------------------------

    def _doc_path(self, doc_id: Any) -> Path:
        return self._path / f"{_id_to_filename(doc_id)}.json"

    def _all_docs(self) -> list:
        docs = []
        for fp in self._path.glob("*.json"):
            doc = _load_json(fp)
            if doc is None:
                continue
            if self._is_ttl and _is_expired(doc):
                try:
                    fp.unlink(missing_ok=True)
                except OSError:
                    pass
                continue
            docs.append(doc)
        return docs

    # -- Motor-compatible async API ----------------------------------------

    async def find_one(self, filter_dict: dict, projection: dict = None) -> Optional[dict]:
        # Fast path: single _id lookup avoids full scan
        keys = list(filter_dict.keys())
        if keys == ["_id"] and not isinstance(filter_dict["_id"], dict):
            doc = _load_json(self._doc_path(filter_dict["_id"]))
            if doc is None:
                return None
            if self._is_ttl and _is_expired(doc):
                self._doc_path(filter_dict["_id"]).unlink(missing_ok=True)
                return None
            return doc
        # Full scan
        for doc in self._all_docs():
            if _matches(doc, filter_dict):
                return doc
        return None

    def find(self, filter_dict: dict = None, projection: dict = None) -> FileCursor:
        filter_dict = filter_dict or {}
        docs = self._all_docs()
        if filter_dict:
            docs = [d for d in docs if _matches(d, filter_dict)]
        return FileCursor(docs)

    async def insert_one(self, doc: dict) -> None:
        doc_id = doc.get("_id")
        if doc_id is None:
            raise ValueError("Document must have an '_id' field")
        path = self._doc_path(doc_id)
        if path.exists():
            raise DuplicateKeyError(f"Duplicate _id: {doc_id}")
        _save_json(path, doc)

    async def update_one(self, filter_dict: dict, update: dict) -> None:
        # Fast path
        keys = list(filter_dict.keys())
        if keys == ["_id"] and not isinstance(filter_dict["_id"], dict):
            path = self._doc_path(filter_dict["_id"])
            doc = _load_json(path)
            if doc is not None:
                _save_json(path, _apply_update(doc, update))
            return
        for doc in self._all_docs():
            if _matches(doc, filter_dict):
                _save_json(self._doc_path(doc["_id"]), _apply_update(doc, update))
                return

    async def update_many(self, filter_dict: dict, update: dict) -> None:
        for doc in self._all_docs():
            if _matches(doc, filter_dict):
                _save_json(self._doc_path(doc["_id"]), _apply_update(doc, update))

    async def delete_one(self, filter_dict: dict) -> None:
        # Fast path
        keys = list(filter_dict.keys())
        if keys == ["_id"] and not isinstance(filter_dict["_id"], dict):
            self._doc_path(filter_dict["_id"]).unlink(missing_ok=True)
            return
        for fp in self._path.glob("*.json"):
            doc = _load_json(fp)
            if doc and _matches(doc, filter_dict):
                fp.unlink(missing_ok=True)
                return

    async def delete_many(self, filter_dict: dict) -> None:
        if not filter_dict:
            for fp in self._path.glob("*.json"):
                fp.unlink(missing_ok=True)
            return
        for fp in self._path.glob("*.json"):
            doc = _load_json(fp)
            if doc and _matches(doc, filter_dict):
                fp.unlink(missing_ok=True)

    async def count_documents(self, filter_dict: dict) -> int:
        return sum(1 for d in self._all_docs() if _matches(d, filter_dict))

    async def create_index(self, keys, **kwargs) -> None:
        pass  # No-op — file backend has no indexes


class DuplicateKeyError(Exception):
    """Raised by FileCollection.insert_one when _id already exists."""
    pass


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

class FileDatabase:
    """Motor-compatible database backed by a directory of JSON files."""

    def __init__(self, data_dir: str):
        self._base = Path(data_dir).resolve()
        self._base.mkdir(parents=True, exist_ok=True)
        self._collections: dict[str, FileCollection] = {}

    def __getitem__(self, name: str) -> FileCollection:
        if name not in self._collections:
            self._collections[name] = FileCollection(self._base, name)
        return self._collections[name]
