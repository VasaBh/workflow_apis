"""
Seed script — populates the file-system data directory with sample data.

Run from the project root:
    python scripts/seed.py
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import asyncio
import bcrypt as _bcrypt
from datetime import datetime, timezone
from app.db.files import FileDatabase, DuplicateKeyError

DATA_DIR = "./data"


def hash_password(password: str) -> str:
    return _bcrypt.hashpw(password.encode(), _bcrypt.gensalt()).decode()


def ts(y, m, d, h=0, mi=0):
    return datetime(y, m, d, h, mi, 0, tzinfo=timezone.utc).isoformat()


async def seed():
    db = FileDatabase(DATA_DIR)

    # ------------------------------------------------------------------ users
    print("Seeding users...")

    users = [
        {
            "_id":           "user-admin-001",
            "name":          "admin",
            "email":         "admin@example.com",
            "password_hash": hash_password("admin"),   # bypass API length check for seed
            "role":          "admin",
            "created_at":    ts(2026, 1, 1),
            "updated_at":    ts(2026, 1, 1),
        },
        {
            "_id":           "user-editor-001",
            "name":          "Alice Editor",
            "email":         "alice@example.com",
            "password_hash": hash_password("password123"),
            "role":          "editor",
            "created_at":    ts(2026, 1, 2),
            "updated_at":    ts(2026, 1, 2),
        },
        {
            "_id":           "user-executor-001",
            "name":          "Bob Executor",
            "email":         "bob@example.com",
            "password_hash": hash_password("password123"),
            "role":          "executor",
            "created_at":    ts(2026, 1, 3),
            "updated_at":    ts(2026, 1, 3),
        },
        {
            "_id":           "user-viewer-001",
            "name":          "Carol Viewer",
            "email":         "carol@example.com",
            "password_hash": hash_password("password123"),
            "role":          "viewer",
            "created_at":    ts(2026, 1, 4),
            "updated_at":    ts(2026, 1, 4),
        },
    ]

    for u in users:
        try:
            await db["users"].insert_one(u)
            print(f"  + user: {u['email']}")
        except DuplicateKeyError:
            print(f"  ~ user already exists: {u['email']}")

    # ----------------------------------------------------------------- scripts
    print("Seeding scripts...")

    scripts = [
        {
            "_id":         "script-001",
            "name":        "Data Ingestion",
            "description": "Ingests rows from a configurable source and returns a count",
            "language":    "python",
            "entry":       "main",
            "code":        (
                "def main(params):\n"
                "    source = params.get('source', 's3')\n"
                "    rows = params.get('row_count', 100)\n"
                "    print(f'[INFO] Ingesting {rows} rows from {source}')\n"
                "    return {'rows_ingested': rows, 'source': source}\n"
            ),
            "parameters": [
                {"name": "source",    "type": "string",  "required": True,  "default": "s3",  "description": "Source identifier"},
                {"name": "row_count", "type": "integer", "required": False, "default": "100", "description": "Number of rows to ingest"},
            ],
            "version":    1,
            "created_at": ts(2026, 1, 5),
            "updated_at": ts(2026, 1, 5),
            "created_by": "user-admin-001",
        },
        {
            "_id":         "script-002",
            "name":        "Data Validation",
            "description": "Validates that a dataset meets quality rules",
            "language":    "python",
            "entry":       "validate",
            "code":        (
                "def validate(params):\n"
                "    threshold = params.get('threshold', 0.95)\n"
                "    rows = params.get('total_rows', 0)\n"
                "    valid_rows = int(rows * threshold)\n"
                "    passed = valid_rows / rows >= threshold if rows > 0 else False\n"
                "    print(f'[INFO] {valid_rows}/{rows} rows passed validation')\n"
                "    return {'passed': passed, 'valid_rows': valid_rows, 'total_rows': rows}\n"
            ),
            "parameters": [
                {"name": "total_rows", "type": "integer", "required": True,  "default": None,   "description": "Total rows to validate"},
                {"name": "threshold",  "type": "float",   "required": False, "default": "0.95", "description": "Minimum pass rate (0-1)"},
            ],
            "version":    1,
            "created_at": ts(2026, 1, 5, 1),
            "updated_at": ts(2026, 1, 5, 1),
            "created_by": "user-admin-001",
        },
        {
            "_id":         "script-003",
            "name":        "Send Report",
            "description": "Generates and sends a summary report",
            "language":    "python",
            "entry":       "send_report",
            "code":        (
                "def send_report(params):\n"
                "    recipient = params.get('recipient', 'team@example.com')\n"
                "    subject = params.get('subject', 'Workflow Report')\n"
                "    print(f'[INFO] Sending report \"{subject}\" to {recipient}')\n"
                "    return {'sent': True, 'recipient': recipient}\n"
            ),
            "parameters": [
                {"name": "recipient", "type": "string", "required": True,  "default": None,               "description": "Email recipient"},
                {"name": "subject",   "type": "string", "required": False, "default": "Workflow Report",  "description": "Email subject"},
            ],
            "version":    1,
            "created_at": ts(2026, 1, 5, 2),
            "updated_at": ts(2026, 1, 5, 2),
            "created_by": "user-editor-001",
        },
    ]

    for s in scripts:
        try:
            await db["scripts"].insert_one(s)
            print(f"  + script: {s['name']}")
        except DuplicateKeyError:
            print(f"  ~ script already exists: {s['name']}")

    # --------------------------------------------------------------- blueprints
    print("Seeding blueprints...")

    blueprints = [
        {
            "_id":          "blueprint-001",
            "name":         "Daily Data Pipeline",
            "description":  "Ingest data, validate it, and send a report",
            "status":       "published",
            "version":      1,
            "created_at":   ts(2026, 1, 6),
            "updated_at":   ts(2026, 1, 6),
            "published_at": ts(2026, 1, 6, 12),
            "created_by":   "user-admin-001",
        },
        {
            "_id":         "blueprint-002",
            "name":        "Manual Review Workflow",
            "description": "Data ingestion followed by a manual review and approval gate",
            "status":      "draft",
            "version":     1,
            "created_at":  ts(2026, 1, 7),
            "updated_at":  ts(2026, 1, 7),
            "published_at": None,
            "created_by":  "user-editor-001",
        },
    ]

    for b in blueprints:
        try:
            await db["blueprints"].insert_one(b)
            print(f"  + blueprint: {b['name']}")
        except DuplicateKeyError:
            print(f"  ~ blueprint already exists: {b['name']}")

    # ------------------------------------------------------------------- steps
    print("Seeding steps...")

    steps = [
        # Blueprint 001 — published pipeline
        {
            "_id":              "step-001",
            "blueprint_id":     "blueprint-001",
            "name":             "Ingest Data",
            "type":             "script",
            "parent_id":        None,
            "order":            1,
            "script_id":        "script-001",
            "script_params":    {"source": "s3", "row_count": 500},
            "entry":            "main",
            "dependencies":     [],
            "on_failure":       "retry",
            "retry_count":      3,
            "timeout_seconds":  60,
            "validation_rules": [],
            "created_at":       ts(2026, 1, 6, 1),
        },
        {
            "_id":              "step-002",
            "blueprint_id":     "blueprint-001",
            "name":             "Validate Data",
            "type":             "script",
            "parent_id":        None,
            "order":            2,
            "script_id":        "script-002",
            "script_params":    {"total_rows": 500, "threshold": 0.95},
            "entry":            "validate",
            "dependencies":     ["step-001"],
            "on_failure":       "block",
            "retry_count":      0,
            "timeout_seconds":  30,
            "validation_rules": [
                {"type": "required_field", "field": "total_rows", "message": "total_rows is required"}
            ],
            "created_at":       ts(2026, 1, 6, 1),
        },
        {
            "_id":              "step-003",
            "blueprint_id":     "blueprint-001",
            "name":             "Send Report",
            "type":             "script",
            "parent_id":        None,
            "order":            3,
            "script_id":        "script-003",
            "script_params":    {"recipient": "team@example.com"},
            "entry":            "send_report",
            "dependencies":     ["step-002"],
            "on_failure":       "skip",
            "retry_count":      1,
            "timeout_seconds":  20,
            "validation_rules": [],
            "created_at":       ts(2026, 1, 6, 1),
        },
        # Blueprint 002 — draft
        {
            "_id":              "step-004",
            "blueprint_id":     "blueprint-002",
            "name":             "Ingest Raw Data",
            "type":             "script",
            "parent_id":        None,
            "order":            1,
            "script_id":        "script-001",
            "script_params":    {"source": "ftp"},
            "entry":            "main",
            "dependencies":     [],
            "on_failure":       "retry",
            "retry_count":      2,
            "timeout_seconds":  60,
            "validation_rules": [],
            "created_at":       ts(2026, 1, 7, 1),
        },
        {
            "_id":              "step-005",
            "blueprint_id":     "blueprint-002",
            "name":             "Manual Review",
            "type":             "manual",
            "parent_id":        None,
            "order":            2,
            "script_id":        None,
            "script_params":    {},
            "entry":            None,
            "dependencies":     ["step-004"],
            "on_failure":       "block",
            "retry_count":      0,
            "timeout_seconds":  None,
            "validation_rules": [],
            "created_at":       ts(2026, 1, 7, 1),
        },
        {
            "_id":              "step-006",
            "blueprint_id":     "blueprint-002",
            "name":             "Approval Gate",
            "type":             "approval",
            "parent_id":        None,
            "order":            3,
            "script_id":        None,
            "script_params":    {},
            "entry":            None,
            "dependencies":     ["step-005"],
            "on_failure":       "block",
            "retry_count":      0,
            "timeout_seconds":  None,
            "validation_rules": [],
            "created_at":       ts(2026, 1, 7, 1),
        },
    ]

    for s in steps:
        try:
            await db["steps"].insert_one(s)
            print(f"  + step: {s['name']} ({s['blueprint_id']})")
        except DuplicateKeyError:
            print(f"  ~ step already exists: {s['name']}")

    # --------------------------------------------------------------------- runs
    print("Seeding runs...")

    runs = [
        {
            "_id":               "run-001",
            "blueprint_id":      "blueprint-001",
            "blueprint_version": 1,
            "status":            "completed",
            "triggered_by":      "user-admin-001",
            "started_at":        ts(2026, 3, 1, 9, 0),
            "completed_at":      ts(2026, 3, 1, 9, 5),
            "created_at":        ts(2026, 3, 1, 9, 0),
        },
        {
            "_id":               "run-002",
            "blueprint_id":      "blueprint-001",
            "blueprint_version": 1,
            "status":            "failed",
            "triggered_by":      "user-executor-001",
            "started_at":        ts(2026, 3, 2, 9, 0),
            "completed_at":      ts(2026, 3, 2, 9, 2),
            "created_at":        ts(2026, 3, 2, 9, 0),
        },
        {
            "_id":               "run-003",
            "blueprint_id":      "blueprint-001",
            "blueprint_version": 1,
            "status":            "not_started",
            "triggered_by":      "user-admin-001",
            "started_at":        None,
            "completed_at":      None,
            "created_at":        ts(2026, 4, 4, 8, 0),
        },
    ]

    for r in runs:
        try:
            await db["runs"].insert_one(r)
            print(f"  + run: {r['_id']} ({r['status']})")
        except DuplicateKeyError:
            print(f"  ~ run already exists: {r['_id']}")

    # --------------------------------------------------------------- step_runs
    print("Seeding step_runs...")

    step_runs = [
        # run-001 — completed
        {"_id": "sr-001-1", "run_id": "run-001", "step_id": "step-001", "name": "Ingest Data",    "type": "script",   "parent_id": None, "order": 1, "status": "completed", "output": {"rows_ingested": 500, "source": "s3"}, "error": None, "logs": ["[INFO] Ingesting 500 rows from s3"], "started_at": ts(2026, 3, 1, 9, 0), "completed_at": ts(2026, 3, 1, 9, 1), "approved_by": None, "approved_at": None},
        {"_id": "sr-001-2", "run_id": "run-001", "step_id": "step-002", "name": "Validate Data",  "type": "script",   "parent_id": None, "order": 2, "status": "completed", "output": {"passed": True, "valid_rows": 475, "total_rows": 500}, "error": None, "logs": ["[INFO] 475/500 rows passed validation"], "started_at": ts(2026, 3, 1, 9, 1), "completed_at": ts(2026, 3, 1, 9, 3), "approved_by": None, "approved_at": None},
        {"_id": "sr-001-3", "run_id": "run-001", "step_id": "step-003", "name": "Send Report",    "type": "script",   "parent_id": None, "order": 3, "status": "completed", "output": {"sent": True, "recipient": "team@example.com"}, "error": None, "logs": ["[INFO] Sending report to team@example.com"], "started_at": ts(2026, 3, 1, 9, 3), "completed_at": ts(2026, 3, 1, 9, 5), "approved_by": None, "approved_at": None},
        # run-002 — failed at step 2
        {"_id": "sr-002-1", "run_id": "run-002", "step_id": "step-001", "name": "Ingest Data",    "type": "script",   "parent_id": None, "order": 1, "status": "completed", "output": {"rows_ingested": 0, "source": "s3"}, "error": None, "logs": ["[INFO] Ingesting 0 rows from s3"], "started_at": ts(2026, 3, 2, 9, 0), "completed_at": ts(2026, 3, 2, 9, 1), "approved_by": None, "approved_at": None},
        {"_id": "sr-002-2", "run_id": "run-002", "step_id": "step-002", "name": "Validate Data",  "type": "script",   "parent_id": None, "order": 2, "status": "failed",    "output": None, "error": "ZeroDivisionError: division by zero", "logs": ["[INFO] 0/0 rows passed validation"], "started_at": ts(2026, 3, 2, 9, 1), "completed_at": ts(2026, 3, 2, 9, 2), "approved_by": None, "approved_at": None},
        {"_id": "sr-002-3", "run_id": "run-002", "step_id": "step-003", "name": "Send Report",    "type": "script",   "parent_id": None, "order": 3, "status": "not_started","output": None, "error": None, "logs": [], "started_at": None, "completed_at": None, "approved_by": None, "approved_at": None},
        # run-003 — not started yet
        {"_id": "sr-003-1", "run_id": "run-003", "step_id": "step-001", "name": "Ingest Data",    "type": "script",   "parent_id": None, "order": 1, "status": "not_started","output": None, "error": None, "logs": [], "started_at": None, "completed_at": None, "approved_by": None, "approved_at": None},
        {"_id": "sr-003-2", "run_id": "run-003", "step_id": "step-002", "name": "Validate Data",  "type": "script",   "parent_id": None, "order": 2, "status": "not_started","output": None, "error": None, "logs": [], "started_at": None, "completed_at": None, "approved_by": None, "approved_at": None},
        {"_id": "sr-003-3", "run_id": "run-003", "step_id": "step-003", "name": "Send Report",    "type": "script",   "parent_id": None, "order": 3, "status": "not_started","output": None, "error": None, "logs": [], "started_at": None, "completed_at": None, "approved_by": None, "approved_at": None},
    ]

    for sr in step_runs:
        try:
            await db["step_runs"].insert_one(sr)
            print(f"  + step_run: {sr['_id']}")
        except DuplicateKeyError:
            print(f"  ~ step_run already exists: {sr['_id']}")

    # --------------------------------------------------------------- schedules
    print("Seeding schedules...")

    schedules = [
        {
            "_id":             "schedule-001",
            "name":            "Daily 9am Pipeline",
            "blueprint_id":    "blueprint-001",
            "cron_expression": "0 9 * * *",
            "timezone":        "UTC",
            "context":         {"env": "production"},
            "status":          "active",
            "last_run_at":     ts(2026, 4, 3, 9, 0),
            "next_run_at":     ts(2026, 4, 4, 9, 0),
            "created_at":      ts(2026, 1, 10),
        },
        {
            "_id":             "schedule-002",
            "name":            "Weekly Sunday Report",
            "blueprint_id":    "blueprint-001",
            "cron_expression": "0 8 * * 0",
            "timezone":        "UTC",
            "context":         {"env": "production", "report_type": "weekly"},
            "status":          "paused",
            "last_run_at":     ts(2026, 3, 29, 8, 0),
            "next_run_at":     None,
            "created_at":      ts(2026, 1, 11),
        },
    ]

    for s in schedules:
        try:
            await db["schedules"].insert_one(s)
            print(f"  + schedule: {s['name']}")
        except DuplicateKeyError:
            print(f"  ~ schedule already exists: {s['name']}")

    # ----------------------------------------------------------- notifications
    print("Seeding notifications...")

    notifications = [
        {
            "_id":          "notif-001",
            "user_id":      "user-admin-001",
            "event_type":   "run_completed",
            "title":        "Run Completed",
            "message":      "Daily Data Pipeline run-001 completed successfully",
            "read":         True,
            "created_at":   ts(2026, 3, 1, 9, 5),
            "reference_id": "run-001",
        },
        {
            "_id":          "notif-002",
            "user_id":      "user-admin-001",
            "event_type":   "run_failed",
            "title":        "Run Failed",
            "message":      "Daily Data Pipeline run-002 failed at step Validate Data",
            "read":         False,
            "created_at":   ts(2026, 3, 2, 9, 2),
            "reference_id": "run-002",
        },
        {
            "_id":          "notif-003",
            "user_id":      "user-executor-001",
            "event_type":   "run_failed",
            "title":        "Run Failed",
            "message":      "Daily Data Pipeline run-002 failed",
            "read":         False,
            "created_at":   ts(2026, 3, 2, 9, 2),
            "reference_id": "run-002",
        },
        {
            "_id":          "notif-004",
            "user_id":      "user-admin-001",
            "event_type":   "approval_required",
            "title":        "Approval Required",
            "message":      "Step 'Approval Gate' in run-003 is waiting for your approval",
            "read":         False,
            "created_at":   ts(2026, 4, 4, 8, 30),
            "reference_id": "sr-003-3",
        },
    ]

    for n in notifications:
        try:
            await db["notifications"].insert_one(n)
            print(f"  + notification: {n['_id']} ({n['event_type']})")
        except DuplicateKeyError:
            print(f"  ~ notification already exists: {n['_id']}")

    # ---------------------------------------------------------------- webhooks
    print("Seeding webhooks...")

    webhooks = [
        {
            "_id":        "webhook-001",
            "name":       "Slack Alerts",
            "url":        "https://hooks.slack.com/services/EXAMPLE/WEBHOOK/URL",
            "events":     ["run_completed", "run_failed", "approval_required"],
            "secret":     "slack-hmac-secret",
            "active":     True,
            "created_at": ts(2026, 1, 15),
        },
        {
            "_id":        "webhook-002",
            "name":       "CI/CD Trigger",
            "url":        "https://ci.example.com/hooks/workflow",
            "events":     ["run_completed"],
            "secret":     "ci-hmac-secret",
            "active":     False,
            "created_at": ts(2026, 2, 1),
        },
    ]

    for w in webhooks:
        try:
            await db["webhooks"].insert_one(w)
            print(f"  + webhook: {w['name']}")
        except DuplicateKeyError:
            print(f"  ~ webhook already exists: {w['name']}")

    print("\nDone! Sample data written to", DATA_DIR)
    print("\nLogin credentials:")
    print("  admin@example.com   / admin")
    print("  alice@example.com   / password123  (editor)")
    print("  bob@example.com     / password123  (executor)")
    print("  carol@example.com   / password123  (viewer)")


if __name__ == "__main__":
    asyncio.run(seed())
