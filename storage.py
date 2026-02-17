import json
import sqlite3
from datetime import datetime, timezone
from typing import Any


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS client_status (
    sf_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    task_name TEXT NOT NULL,
    task_status TEXT NOT NULL,
    task_url TEXT NOT NULL,
    task_created_at TEXT,
    task_closed_at TEXT,
    metrics_json TEXT NOT NULL,
    source_updated_at TEXT,
    synced_at TEXT NOT NULL
);
"""

CREATE_OVERRIDES_SQL = """
CREATE TABLE IF NOT EXISTS ecd_override (
    sf_id TEXT NOT NULL,
    step_slug TEXT NOT NULL,
    ecd_value TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (sf_id, step_slug)
);
"""

CREATE_EDIT_LOG_SQL = """
CREATE TABLE IF NOT EXISTS edit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    logged_at TEXT NOT NULL,
    sf_id TEXT NOT NULL,
    task_id TEXT,
    field_key TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    source TEXT NOT NULL
);
"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(CREATE_TABLE_SQL)
        conn.execute(CREATE_OVERRIDES_SQL)
        conn.execute(CREATE_EDIT_LOG_SQL)
        _ensure_column(conn, "task_created_at", "TEXT")
        _ensure_column(conn, "task_closed_at", "TEXT")
        conn.commit()


def _ensure_column(conn: sqlite3.Connection, column_name: str, column_type: str) -> None:
    existing = {
        row[1]
        for row in conn.execute("PRAGMA table_info(client_status)").fetchall()
    }
    if column_name not in existing:
        conn.execute(f"ALTER TABLE client_status ADD COLUMN {column_name} {column_type}")


def upsert_client_status(db_path: str, row: dict[str, Any]) -> None:
    sql = """
    INSERT INTO client_status (
        sf_id, task_id, task_name, task_status, task_url,
        task_created_at, task_closed_at, metrics_json, source_updated_at, synced_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(sf_id) DO UPDATE SET
        task_id=excluded.task_id,
        task_name=excluded.task_name,
        task_status=excluded.task_status,
        task_url=excluded.task_url,
        task_created_at=excluded.task_created_at,
        task_closed_at=excluded.task_closed_at,
        metrics_json=excluded.metrics_json,
        source_updated_at=excluded.source_updated_at,
        synced_at=excluded.synced_at;
    """

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            sql,
            (
                row["sf_id"],
                row["task_id"],
                row["task_name"],
                row["task_status"],
                row["task_url"],
                row.get("task_created_at", ""),
                row.get("task_closed_at", ""),
                json.dumps(row.get("metrics", {}), ensure_ascii=True),
                row.get("source_updated_at", ""),
                utc_now_iso(),
            ),
        )
        conn.commit()


def upsert_many_client_statuses(db_path: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    sql = """
    INSERT INTO client_status (
        sf_id, task_id, task_name, task_status, task_url,
        task_created_at, task_closed_at, metrics_json, source_updated_at, synced_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(sf_id) DO UPDATE SET
        task_id=excluded.task_id,
        task_name=excluded.task_name,
        task_status=excluded.task_status,
        task_url=excluded.task_url,
        task_created_at=excluded.task_created_at,
        task_closed_at=excluded.task_closed_at,
        metrics_json=excluded.metrics_json,
        source_updated_at=excluded.source_updated_at,
        synced_at=excluded.synced_at;
    """

    payload = [
        (
            row["sf_id"],
            row["task_id"],
            row["task_name"],
            row["task_status"],
            row["task_url"],
            row.get("task_created_at", ""),
            row.get("task_closed_at", ""),
            json.dumps(row.get("metrics", {}), ensure_ascii=True),
            row.get("source_updated_at", ""),
            utc_now_iso(),
        )
        for row in rows
    ]

    with sqlite3.connect(db_path) as conn:
        conn.executemany(sql, payload)
        conn.commit()

    return len(payload)


def get_client_status(db_path: str, sf_id: str) -> dict[str, Any] | None:
    query = """
    SELECT sf_id, task_id, task_name, task_status, task_url,
           task_created_at, task_closed_at, metrics_json, source_updated_at, synced_at
    FROM client_status
    WHERE sf_id = ?
    """

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(query, (sf_id,)).fetchone()

    if not row:
        return None

    return {
        "sf_id": row[0],
        "task_id": row[1],
        "task_name": row[2],
        "task_status": row[3],
        "task_url": row[4],
        "task_created_at": row[5],
        "task_closed_at": row[6],
        "metrics": json.loads(row[7] or "{}"),
        "source_updated_at": row[8],
        "synced_at": row[9],
    }


def list_client_statuses(db_path: str) -> list[dict[str, Any]]:
    query = """
    SELECT sf_id, task_id, task_name, task_status, task_url,
           task_created_at, task_closed_at, metrics_json, source_updated_at, synced_at
    FROM client_status
    ORDER BY lower(task_name), sf_id
    """
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(query).fetchall()

    output: list[dict[str, Any]] = []
    for row in rows:
        output.append(
            {
                "sf_id": row[0],
                "task_id": row[1],
                "task_name": row[2],
                "task_status": row[3],
                "task_url": row[4],
                "task_created_at": row[5],
                "task_closed_at": row[6],
                "metrics": json.loads(row[7] or "{}"),
                "source_updated_at": row[8],
                "synced_at": row[9],
            }
        )
    return output


def get_ecd_overrides(db_path: str, sf_id: str) -> dict[str, str]:
    query = """
    SELECT step_slug, ecd_value
    FROM ecd_override
    WHERE sf_id = ?
    """
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(query, (sf_id,)).fetchall()
    return {row[0]: row[1] for row in rows}


def upsert_ecd_override(db_path: str, sf_id: str, step_slug: str, ecd_value: str) -> None:
    with sqlite3.connect(db_path) as conn:
        if str(ecd_value).strip():
            conn.execute(
                """
                INSERT INTO ecd_override (sf_id, step_slug, ecd_value, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(sf_id, step_slug) DO UPDATE SET
                    ecd_value=excluded.ecd_value,
                    updated_at=excluded.updated_at
                """,
                (sf_id, step_slug, ecd_value, utc_now_iso()),
            )
        else:
            conn.execute(
                "DELETE FROM ecd_override WHERE sf_id = ? AND step_slug = ?",
                (sf_id, step_slug),
            )
        conn.commit()


def log_edit(
    db_path: str,
    sf_id: str,
    task_id: str,
    field_key: str,
    old_value: str,
    new_value: str,
    source: str,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO edit_log (logged_at, sf_id, task_id, field_key, old_value, new_value, source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (utc_now_iso(), sf_id, task_id, field_key, old_value, new_value, source),
        )
        conn.commit()
