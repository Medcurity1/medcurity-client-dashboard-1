from datetime import datetime, timezone
from typing import Any

import requests


REQUEST_TIMEOUT = 25


def clickup_headers(api_token: str) -> dict[str, str]:
    return {
        "Authorization": api_token,
        "Content-Type": "application/json",
    }


def fetch_tasks_for_list(api_token: str, list_id: str) -> list[dict[str, Any]]:
    url = f"https://api.clickup.com/api/v2/list/{list_id}/task"
    tasks: list[dict[str, Any]] = []
    page = 0

    while True:
        response = requests.get(
            url,
            headers=clickup_headers(api_token),
            params={"page": page, "subtasks": "true", "include_closed": "true"},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()

        current = data.get("tasks", [])
        tasks.extend(current)

        if data.get("last_page") is True or not current:
            break

        page += 1

    return tasks


def fetch_task_by_id(api_token: str, task_id: str) -> dict[str, Any]:
    url = f"https://api.clickup.com/api/v2/task/{task_id}"
    response = requests.get(
        url,
        headers=clickup_headers(api_token),
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def fetch_latest_task_comment(api_token: str, task_id: str) -> str:
    url = f"https://api.clickup.com/api/v2/task/{task_id}/comment"
    response = requests.get(
        url,
        headers=clickup_headers(api_token),
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    comments = response.json().get("comments", [])
    if not comments:
        return ""

    latest = max(comments, key=lambda item: int(item.get("date", "0")))
    text = str(latest.get("comment_text", "")).strip()
    return text


def set_task_custom_field_value(
    api_token: str,
    task_id: str,
    field_id: str,
    value: Any,
) -> None:
    url = f"https://api.clickup.com/api/v2/task/{task_id}/field/{field_id}"
    response = requests.post(
        url,
        headers=clickup_headers(api_token),
        json={"value": value},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()


def custom_field(task: dict[str, Any], field_id: str) -> dict[str, Any] | None:
    for field in task.get("custom_fields", []):
        if field.get("id") == field_id:
            return field
    return None


def format_clickup_date(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        dt = datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
        return dt.strftime("%m/%d/%Y")
    except (TypeError, ValueError):
        return ""


def format_dropdown(field: dict[str, Any]) -> str:
    value = field.get("value")
    options = (field.get("type_config") or {}).get("options", [])
    if value in (None, ""):
        return ""

    for option in options:
        if str(option.get("id")) == str(value):
            return str(option.get("name", ""))
        if str(option.get("orderindex")) == str(value):
            return str(option.get("name", ""))

    return str(value)


def format_custom_field(field: dict[str, Any] | None) -> str:
    if not field:
        return ""

    field_type = field.get("type")
    value = field.get("value")
    if field_type == "date":
        return format_clickup_date(value)
    if field_type == "drop_down":
        return format_dropdown(field)

    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    if isinstance(value, list):
        return ", ".join(str(v) for v in value)
    if isinstance(value, dict):
        return "; ".join(f"{k}: {v}" for k, v in value.items())
    return str(value)


def ms_to_iso(ms_value: Any) -> str:
    if not ms_value:
        return ""
    try:
        dt = datetime.fromtimestamp(int(ms_value) / 1000, tz=timezone.utc)
        return dt.isoformat()
    except (TypeError, ValueError):
        return ""


def normalize_task(
    task: dict[str, Any],
    sf_id_field_id: str,
    field_map: dict[str, str],
) -> dict[str, Any] | None:
    sf_field = custom_field(task, sf_id_field_id)
    sf_id_raw = sf_field.get("value") if sf_field else None
    sf_id = str(sf_id_raw).strip() if sf_id_raw not in (None, "") else ""
    if not sf_id:
        return None

    metrics: dict[str, str] = {}
    for label, field_id in field_map.items():
        metrics[label] = format_custom_field(custom_field(task, field_id))

    return {
        "sf_id": sf_id,
        "task_id": str(task.get("id", "")),
        "task_name": str(task.get("name", "")),
        "task_status": str((task.get("status") or {}).get("status", "")),
        "task_closed": (task.get("status") or {}).get("type", "") == "closed",
        "task_url": str(task.get("url", "")),
        "task_created_at": ms_to_iso(task.get("date_created")),
        "task_closed_at": ms_to_iso(task.get("date_closed") or task.get("date_done")),
        "metrics": metrics,
        "source_updated_at": ms_to_iso(task.get("date_updated")),
    }
