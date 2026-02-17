import json
import os
from pathlib import Path


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return

    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def parse_field_map(raw: str) -> dict[str, str]:
    if not raw.strip():
        return {}

    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("CLICKUP_FIELD_MAP_JSON must be valid JSON") from exc

    if not isinstance(loaded, dict):
        raise RuntimeError("CLICKUP_FIELD_MAP_JSON must be a JSON object")

    field_map: dict[str, str] = {}
    for label, field_id in loaded.items():
        label_clean = str(label).strip()
        field_clean = str(field_id).strip()
        if label_clean and field_clean:
            field_map[label_clean] = field_clean

    return field_map


load_dotenv(Path(__file__).with_name(".env"))

CLICKUP_API_TOKEN = required_env("CLICKUP_API_TOKEN")
CLICKUP_LIST_ID = required_env("CLICKUP_LIST_ID")
CLICKUP_SF_ID_FIELD_ID = required_env("CLICKUP_SF_ID_FIELD_ID")
CLIENT_LINK_SECRET = required_env("CLIENT_LINK_SECRET")

ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "")
DATABASE_PATH = os.getenv("DATABASE_PATH", "client_status.db")
PORT = int(os.getenv("PORT", "8080"))

CLICKUP_FIELD_MAP = parse_field_map(os.getenv("CLICKUP_FIELD_MAP_JSON", "{}"))
