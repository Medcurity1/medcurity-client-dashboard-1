import hashlib
import hmac
import io
import re
from collections import defaultdict
import csv
from datetime import date, datetime, timedelta, timezone

from flask import Flask, abort, jsonify, redirect, render_template, request, url_for

from clickup_service import (
    fetch_latest_task_comment,
    fetch_task_by_id,
    fetch_tasks_for_list,
    normalize_task,
    set_task_custom_field_value,
)
from config import (
    ADMIN_API_KEY,
    CLICKUP_API_TOKEN,
    CLICKUP_FIELD_MAP,
    CLICKUP_LIST_ID,
    CLICKUP_SF_ID_FIELD_ID,
    CLIENT_LINK_SECRET,
    DATABASE_PATH,
    PORT,
    WEBHOOK_TOKEN,
)
from storage import (
    get_client_status,
    get_acd_anchor_preferences,
    get_ecd_overrides,
    init_db,
    latest_source_updated_at,
    list_historical_close_metrics,
    list_client_statuses,
    log_edit,
    upsert_historical_close_metrics,
    upsert_acd_anchor_preference,
    upsert_ecd_override,
    upsert_client_status,
    upsert_many_client_statuses,
)


app = Flask(__name__)
init_db(DATABASE_PATH)

SRA_ECD_OFFSETS_DAYS = {
    "receive_policies_and_procedures_baa": 7,
    "review_policies_and_procedures_baa": 19,
    "schedule_onsite_remote_interview": 14,
    "go_onsite_have_interview": 21,
    "recieve_requested_follow_up_documentation": 28,
    "review_sra": 35,
    "schedule_final_sra_report": 42,
    "present_final_sra_report": 49,
}

NVA_ECD_OFFSETS_DAYS = {
    "receive_credentials": 7,
    "verify_access": 14,
    "scans_complete": 21,
    "access_removed": 28,
    "compile_report": 35,
    "schedule_final_nva_report": 42,
    "present_final_nva_report": 49,
}

TRACKED_MAX_CLOSE_DAYS = 120


def to_title_case(raw: str) -> str:
    cleaned = str(raw).replace("_", " ").replace("-", " ").strip()
    if not cleaned:
        return ""
    return " ".join(part.capitalize() for part in cleaned.split())


def apply_acronyms(text: str) -> str:
    value = text
    replacements = {
        "Sra": "SRA",
        "Nva": "NVA",
        "Baa": "BAA",
        "Ecd": "ECD",
        "Acd": "ACD",
    }
    for source, target in replacements.items():
        value = value.replace(source, target)
    return value


def project_field_label(field_slug: str) -> str:
    labels = {
        "project_lead": "Project Lead",
        "project_support": "Project Support",
        "remote_onsite": "Location",
        "time_zone": "Time Zone",
        "contract_signed": "Contract Signed",
        "status": "Status",
        "ecd": "ECD",
        "acd": "ACD",
    }
    return labels.get(field_slug, apply_acronyms(to_title_case(field_slug)))


def step_title(section: str, step_slug: str, location_value: str) -> str:
    location_text = location_value.lower()
    has_onsite = "onsite" in location_text
    has_remote = "remote" in location_text

    if step_slug == "schedule_onsite_remote_interview":
        if has_onsite and not has_remote:
            return "Schedule Onsite Visit"
        if has_remote and not has_onsite:
            return "Schedule Interview Sessions"
        return "Schedule Onsite/Remote Interview"

    if step_slug == "go_onsite_have_interview":
        if has_onsite and not has_remote:
            return "Go Onsite/Have Interviews"
        if has_remote and not has_onsite:
            return "Conduct Interview Sessions"
        return "Go Onsite/Have Interviews"

    labels = {
        "sra_kickoff": "SRA Kickoff",
        "receive_policies_and_procedures_baa": "Receive Policies and Procedures / BAA",
        "review_policies_and_procedures_baa": "Review Policies and Procedures / BAA",
        "go_onsite_have_interview": "Go Onsite/Have Interviews",
        "recieve_requested_follow_up_documentation": "Receive Requested Follow Up Documentation",
        "review_sra": "Review SRA",
        "schedule_final_sra_report": "Schedule Final SRA Report",
        "present_final_sra_report": "Present Final SRA Report",
        "nva_kickoff": "NVA Kickoff",
        "receive_credentials": "Receive Credentials",
        "verify_access": "Verify Access",
        "scans_complete": "Scans Complete",
        "access_removed": "Access Removed",
        "compile_report": "Compile Report",
        "schedule_final_nva_report": "Schedule Final NVA Report",
        "present_final_nva_report": "Present Final NVA Report",
    }
    fallback = apply_acronyms(to_title_case(step_slug))
    return labels.get(step_slug, fallback)


def step_owner(section: str, step_slug: str, client_name: str) -> str:
    if section == "sra":
        sra_owner_rules = {
            "sra_kickoff": "Medcurity",
            "receive_policies_and_procedures_baa": client_name,
            "review_policies_and_procedures_baa": "Medcurity",
            "schedule_onsite_remote_interview": f"Medcurity & {client_name}",
            "go_onsite_have_interview": "Medcurity",
            "recieve_requested_follow_up_documentation": client_name,
            "review_sra": "Medcurity",
            "schedule_final_sra_report": f"Medcurity & {client_name}",
            "present_final_sra_report": "Medcurity",
        }
        return sra_owner_rules.get(step_slug, client_name)

    # Keep NVA owner default simple until NVA matrix rules are provided.
    return client_name


def parse_bool(value) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "checked"}:
        return True
    if text in {"false", "0", "no", "n", "unchecked", ""}:
        return False
    return None


def parse_us_date(value: str) -> datetime | None:
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%m/%d/%Y")
    except ValueError:
        return None


def format_us_date(value: datetime) -> str:
    return value.strftime("%m/%d/%Y")


def shift_to_monday_if_weekend(value: datetime) -> datetime:
    weekday = value.weekday()  # Mon=0 ... Sun=6
    if weekday == 5:
        return value + timedelta(days=2)
    if weekday == 6:
        return value + timedelta(days=1)
    return value


def shift_to_friday_if_weekend(value: datetime) -> datetime:
    weekday = value.weekday()  # Mon=0 ... Sun=6
    if weekday == 5:
        return value - timedelta(days=1)
    if weekday == 6:
        return value - timedelta(days=2)
    return value


def next_business_day(value: datetime) -> datetime:
    candidate = value + timedelta(days=1)
    return shift_to_monday_if_weekend(candidate)


def us_to_ymd(value: str) -> str:
    parsed = parse_us_date(value)
    return parsed.strftime("%Y-%m-%d") if parsed else ""


def ymd_to_clickup_ms(value: str) -> int:
    dt = datetime.strptime(value, "%Y-%m-%d")
    return int(dt.replace(hour=12, minute=0, second=0, microsecond=0).timestamp() * 1000)


def parse_iso_or_min(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min


def format_dt_to_us(dt: datetime) -> str:
    if dt == datetime.min:
        return "Unknown"
    return dt.strftime("%m/%d/%Y")


def year_quarter_label_from_dt(dt: datetime) -> str:
    if dt == datetime.min:
        return "Unknown"
    quarter = ((dt.month - 1) // 3) + 1
    return f"{dt.year} Q{quarter}"


def group_projects_for_admin(projects: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for project in projects:
        key = project["period_label"]
        grouped.setdefault(key, []).append(project)
    # Sort periods descending by parsed date where possible.
    def sort_key(label: str) -> tuple[int, int]:
        if label == "Unknown":
            return (-1, -1)
        parts = label.split()
        year = int(parts[0])
        quarter = int(parts[1].replace("Q", ""))
        return (year, quarter)
    ordered_keys = sorted(grouped.keys(), key=sort_key, reverse=True)
    return {k: grouped[k] for k in ordered_keys}


def parse_metric_us_date(value: str) -> datetime:
    parsed = parse_us_date(value)
    return parsed if parsed else datetime.min


DATE_TOKEN_RE = re.compile(r"(\d{1,2}/\d{1,2}/\d{2,4})")


def parse_any_us_date(value: str) -> datetime | None:
    text = str(value).strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered in {"na", "n/a", "tbc", "not applicable", "none", "not set"}:
        return None
    direct = parse_us_date(text)
    if direct:
        return direct
    match = DATE_TOKEN_RE.search(text)
    if not match:
        return None
    token = match.group(1)
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(token, fmt)
        except ValueError:
            continue
    return None


def business_days_between(start_dt: datetime | None, end_dt: datetime | None) -> int | None:
    if not start_dt or not end_dt:
        return None
    start = start_dt.date()
    end = end_dt.date()
    if end < start:
        return None
    count = 0
    current = start
    while current <= end:
        if current.weekday() < 5:
            count += 1
        current = current + timedelta(days=1)
    return count


def quarter_label_for_date(dt: datetime) -> str:
    q = ((dt.month - 1) // 3) + 1
    return f"{dt.year} Q{q}"


def metric_first_date(metrics: dict, keys: list[str]) -> datetime | None:
    for key in keys:
        dt = parse_any_us_date(metrics.get(key, ""))
        if dt:
            return dt
    return None


def live_close_metric_rows(rows: list[dict]) -> tuple[list[dict], dict]:
    result: list[dict] = []
    completed_task_total = 0
    completed_task_with_valid_close = 0
    missing_sra_dates = 0
    missing_nva_dates = 0
    missing_company_names: set[str] = set()
    missing_records: list[dict[str, str]] = []
    for row in rows:
        status_clean = str(row.get("task_status", "")).strip().lower()
        if status_clean != "completed":
            continue
        completed_task_total += 1
        metrics = row.get("metrics", {}) or {}
        has_any_valid_for_task = False

        sra_kickoff = metric_first_date(metrics, ["sra.sra_kickoff.date", "sra.sra_kickoff.acd"])
        sra_final = metric_first_date(
            metrics,
            ["sra.present_final_sra_report.date", "sra.present_final_sra_report.acd"],
        )
        sra_days = business_days_between(sra_kickoff, sra_final)
        sra_enabled = parse_bool(
            metrics.get("project.sra_enabled")
            or metrics.get("sra.enabled")
            or metrics.get("sra_enabled")
        )
        sra_relevant = sra_enabled is True or any(str(k).startswith("sra.") for k in metrics.keys())
        if sra_days:
            has_any_valid_for_task = True
            result.append(
                {
                    "company": row.get("task_name", ""),
                    "track": "SRA",
                    "kickoff_date": format_us_date(sra_kickoff),
                    "final_date": format_us_date(sra_final),
                    "close_days": sra_days,
                    "quarter_label": quarter_label_for_date(sra_final),
                    "source": "clickup_live",
                }
            )
        elif sra_relevant:
            missing_sra_dates += 1

        nva_kickoff = metric_first_date(metrics, ["nva.nva_kickoff.date", "nva.nva_kickoff.acd"])
        nva_final = metric_first_date(
            metrics,
            ["nva.present_final_nva_report.date", "nva.present_final_nva_report.acd"],
        )
        nva_days = business_days_between(nva_kickoff, nva_final)
        nva_enabled = parse_bool(
            metrics.get("project.nva_enabled")
            or metrics.get("nva.enabled")
            or metrics.get("nva_enabled")
        )
        nva_relevant = nva_enabled is True or any(str(k).startswith("nva.") for k in metrics.keys())
        if nva_days:
            has_any_valid_for_task = True
            result.append(
                {
                    "company": row.get("task_name", ""),
                    "track": "NVA",
                    "kickoff_date": format_us_date(nva_kickoff),
                    "final_date": format_us_date(nva_final),
                    "close_days": nva_days,
                    "quarter_label": quarter_label_for_date(nva_final),
                    "source": "clickup_live",
                }
            )
        elif nva_relevant:
            missing_nva_dates += 1

        if has_any_valid_for_task:
            completed_task_with_valid_close += 1
        else:
            company = str(row.get("task_name", "")).strip()
            sf_id = str(row.get("sf_id", "")).strip()
            if company:
                missing_company_names.add(company)
            missing_records.append({"company": company, "sf_id": sf_id})

    quality = {
        "completed_task_total": completed_task_total,
        "completed_task_with_valid_close": completed_task_with_valid_close,
        "completed_task_missing_close_dates": max(
            completed_task_total - completed_task_with_valid_close,
            0,
        ),
        "missing_sra_dates": missing_sra_dates,
        "missing_nva_dates": missing_nva_dates,
        "missing_companies": sorted(missing_company_names),
        "missing_records": missing_records,
    }
    return result, quality


def summarize_metrics_rows(rows: list[dict]) -> tuple[list[dict], dict]:
    grouped: dict[str, list[int]] = defaultdict(list)
    grouped_sra: dict[str, list[int]] = defaultdict(list)
    grouped_nva: dict[str, list[int]] = defaultdict(list)
    all_days: list[int] = []
    sra_days: list[int] = []
    nva_days: list[int] = []
    tracked_days: list[int] = []

    for row in rows:
        d = int(row["close_days"])
        q = row["quarter_label"]
        t = str(row.get("track", "")).upper()
        grouped[q].append(d)
        all_days.append(d)
        if t == "SRA":
            grouped_sra[q].append(d)
            sra_days.append(d)
        elif t == "NVA":
            grouped_nva[q].append(d)
            nva_days.append(d)
        if d <= TRACKED_MAX_CLOSE_DAYS:
            tracked_days.append(d)

    def quarter_sort_key(label: str) -> tuple[int, int]:
        parts = label.split()
        if len(parts) != 2:
            return (0, 0)
        year = int(parts[0])
        quarter = int(parts[1].replace("Q", ""))
        return (year, quarter)

    quarter_rows: list[dict] = []
    for quarter in sorted(grouped.keys(), key=quarter_sort_key):
        all_vals = grouped[quarter]
        sra_vals = grouped_sra.get(quarter, [])
        nva_vals = grouped_nva.get(quarter, [])
        quarter_rows.append(
            {
                "quarter_label": quarter,
                "project_count": len(all_vals),
                "avg_close_days": round(sum(all_vals) / len(all_vals), 1),
                "sra_count": len(sra_vals),
                "avg_sra_days": round(sum(sra_vals) / len(sra_vals), 1) if sra_vals else None,
                "nva_count": len(nva_vals),
                "avg_nva_days": round(sum(nva_vals) / len(nva_vals), 1) if nva_vals else None,
            }
        )

    summary = {
        "overall_avg_days": round(sum(all_days) / len(all_days), 1) if all_days else None,
        "overall_count": len(all_days),
        "overall_sra_avg_days": round(sum(sra_days) / len(sra_days), 1) if sra_days else None,
        "overall_sra_count": len(sra_days),
        "overall_nva_avg_days": round(sum(nva_days) / len(nva_days), 1) if nva_days else None,
        "overall_nva_count": len(nva_days),
        "overall_median_days": (
            sorted(all_days)[len(all_days) // 2] if all_days else None
        ),
        "overall_p90_days": (
            sorted(all_days)[max((len(all_days) * 9) // 10 - 1, 0)] if all_days else None
        ),
        "tracked_avg_days": (
            round(sum(tracked_days) / len(tracked_days), 1) if tracked_days else None
        ),
        "tracked_count": len(tracked_days),
        "untracked_outlier_count": max(len(all_days) - len(tracked_days), 0),
        "tracked_max_days": TRACKED_MAX_CLOSE_DAYS,
    }
    return quarter_rows, summary


def normalize_header(header: str) -> str:
    return " ".join(str(header).replace("\n", " ").replace("\r", " ").strip().lower().split())


def parse_historical_tsv(tsv_text: str) -> list[dict]:
    raw_text = str(tsv_text or "")
    if not raw_text.strip():
        return []

    def to_int(value: str) -> int | None:
        text = str(value).strip()
        if not text:
            return None
        match = re.search(r"-?\d+", text.replace(",", ""))
        if not match:
            return None
        try:
            return int(match.group(0))
        except ValueError:
            return None

    parsed_rows: list[list[str]] = []
    used_delim = ","
    for delim in [",", "\t"]:
        rows = list(csv.reader(io.StringIO(raw_text), delimiter=delim))
        if not rows:
            continue
        header_idx = -1
        for idx, row in enumerate(rows):
            normalized = [normalize_header(c) for c in row]
            if "company" in normalized and "total days" in normalized:
                header_idx = idx
                break
        if header_idx >= 0:
            parsed_rows = rows[header_idx:]
            used_delim = delim
            break

    if not parsed_rows:
        return []

    headers = parsed_rows[0]
    data_rows = parsed_rows[1:]
    header_lookup = {normalize_header(h): h for h in headers if h}

    company_col = header_lookup.get("company")
    sf_id_col = header_lookup.get("sf id")
    total_days_col = header_lookup.get("total days")
    sra_kickoff_col = header_lookup.get("sra kickoff (sra)")
    sra_final_col = header_lookup.get("present final sra report (sra)")
    nva_kickoff_col = header_lookup.get("nva kickoff (nva)")
    nva_final_col = header_lookup.get("present final nva report (nva)")
    if not company_col or not total_days_col:
        return []

    output: list[dict] = []
    current_quarter_label = ""
    quarter_row_re = re.compile(r"^q([1-4])\s+(\d{4})$", re.IGNORECASE)
    for values in data_rows:
        row = dict(zip(headers, values))
        company = str(row.get(company_col, "")).strip()
        company_l = company.lower()
        quarter_match = quarter_row_re.match(company.strip())
        if quarter_match:
            current_quarter_label = f"{quarter_match.group(2)} Q{quarter_match.group(1)}"
            continue
        if (
            not company
            or company_l.startswith("totals")
            or company_l.startswith("202")
        ):
            continue

        total_days = to_int(row.get(total_days_col, ""))
        if total_days is None or total_days <= 0:
            continue

        sf_id = str(row.get(sf_id_col, "")).strip() if sf_id_col else ""
        sra_kickoff = parse_any_us_date(row.get(sra_kickoff_col, "")) if sra_kickoff_col else None
        sra_final = parse_any_us_date(row.get(sra_final_col, "")) if sra_final_col else None
        nva_kickoff = parse_any_us_date(row.get(nva_kickoff_col, "")) if nva_kickoff_col else None
        nva_final = parse_any_us_date(row.get(nva_final_col, "")) if nva_final_col else None

        kickoff_dt = sra_kickoff or nva_kickoff
        final_dt = sra_final or nva_final
        if not final_dt:
            continue

        if sra_final and nva_final:
            track = "SRA+NVA"
        elif sra_final:
            track = "SRA"
        elif nva_final:
            track = "NVA"
        else:
            track = "Unknown"

        kickoff_label = format_us_date(kickoff_dt) if kickoff_dt else ""
        final_label = format_us_date(final_dt)
        output.append(
            {
                "source_key": f"hist|{company}|{track}|{kickoff_label}|{final_label}|{total_days}|{used_delim}",
                "sf_id": sf_id,
                "company": company,
                "track": track,
                "kickoff_date": kickoff_label,
                "final_date": final_label,
                "close_days": total_days,
                "quarter_label": current_quarter_label or quarter_label_for_date(final_dt),
                "source": "historical_paste",
            }
        )

    return output


def strip_internal_fields(row: dict) -> dict:
    return {k: v for k, v in row.items() if k != "task_closed"}


def refresh_client_from_clickup(sf_id: str) -> bool:
    status = get_client_status(DATABASE_PATH, sf_id)
    if not status:
        return False

    task_id = status.get("task_id", "")
    if not task_id:
        return False

    full_task = fetch_task_by_id(CLICKUP_API_TOKEN, task_id)
    normalized = normalize_task(full_task, CLICKUP_SF_ID_FIELD_ID, CLICKUP_FIELD_MAP)
    if not normalized:
        return False
    normalized["metrics"]["project.next_steps"] = fetch_latest_task_comment(
        CLICKUP_API_TOKEN, task_id
    )

    upsert_client_status(DATABASE_PATH, strip_internal_fields(normalized))
    return True


def refresh_all_from_clickup() -> int:
    tasks = fetch_tasks_for_list(CLICKUP_API_TOKEN, CLICKUP_LIST_ID)
    by_sf_id: dict[str, dict] = {}

    for task in tasks:
        normalized = normalize_task(task, CLICKUP_SF_ID_FIELD_ID, CLICKUP_FIELD_MAP)
        if not normalized:
            continue
        normalized["metrics"]["project.next_steps"] = fetch_latest_task_comment(
            CLICKUP_API_TOKEN, normalized["task_id"]
        )
        sf_id = normalized["sf_id"]
        current = by_sf_id.get(sf_id)
        if current is None or is_better_row(normalized, current):
            by_sf_id[sf_id] = normalized

    return upsert_many_client_statuses(
        DATABASE_PATH, [strip_internal_fields(row) for row in by_sf_id.values()]
    )


def refresh_recent_from_clickup(lookback_minutes: int = 3) -> int:
    latest_iso = latest_source_updated_at(DATABASE_PATH)
    latest_dt = parse_iso_or_min(latest_iso)
    if latest_dt == datetime.min:
        return refresh_all_from_clickup()
    if latest_dt.tzinfo is None:
        latest_dt = latest_dt.replace(tzinfo=timezone.utc)

    since_dt = latest_dt - timedelta(minutes=lookback_minutes)
    since_ms = int(since_dt.timestamp() * 1000)
    tasks = fetch_tasks_for_list(
        CLICKUP_API_TOKEN,
        CLICKUP_LIST_ID,
        date_updated_gt=since_ms,
    )
    if not tasks:
        return 0

    by_sf_id: dict[str, dict] = {}
    for task in tasks:
        normalized = normalize_task(task, CLICKUP_SF_ID_FIELD_ID, CLICKUP_FIELD_MAP)
        if not normalized:
            continue
        normalized["metrics"]["project.next_steps"] = fetch_latest_task_comment(
            CLICKUP_API_TOKEN, normalized["task_id"]
        )
        sf_id = normalized["sf_id"]
        current = by_sf_id.get(sf_id)
        if current is None or is_better_row(normalized, current):
            by_sf_id[sf_id] = normalized

    if not by_sf_id:
        return 0
    return upsert_many_client_statuses(
        DATABASE_PATH, [strip_internal_fields(row) for row in by_sf_id.values()]
    )


def is_better_row(candidate: dict, current: dict) -> bool:
    if candidate.get("task_closed") != current.get("task_closed"):
        return not candidate.get("task_closed")
    return parse_iso_or_min(candidate.get("source_updated_at", "")) >= parse_iso_or_min(
        current.get("source_updated_at", "")
    )


def build_step_display(
    steps: dict[str, dict[str, str]],
    field_keys: dict[str, dict[str, str]],
    step_slugs: dict[str, str],
    can_edit: bool,
) -> dict[str, dict[str, object]]:
    status_classes = {
        "On Track": "status-pill-green",
        "Potential Roadblock": "status-pill-yellow",
        "Roadblock/Overage": "status-pill-red",
        "Completed": "status-pill-green",
        "Not Started": "status-pill-neutral",
    }
    display: dict[str, dict[str, object]] = {}
    for step_name, fields in steps.items():
        owner_value = str(fields.get("Owner", "")).strip()
        status_value = str(fields.get("Status", "")).strip()
        ecd_value = str(fields.get("ECD", "")).strip()
        acd_value = str(fields.get("ACD", "")).strip()

        ecd_key = field_keys.get(step_name, {}).get("ECD", "")
        acd_key = field_keys.get(step_name, {}).get("ACD", "")
        ecd_override_key = field_keys.get(step_name, {}).get("ECD_OVERRIDE", "")
        ecd_editable = can_edit and (
            (bool(ecd_key) and ecd_key in CLICKUP_FIELD_MAP)
            or bool(ecd_override_key)
        )
        acd_editable = can_edit and bool(acd_key) and acd_key in CLICKUP_FIELD_MAP

        extras: list[dict[str, str]] = []
        for label, value in fields.items():
            if label in {"Status", "Owner", "ECD", "ACD"}:
                continue
            extras.append({"label": label, "value": str(value or "")})

        display[step_name] = {
            "step_slug": step_slugs.get(step_name, ""),
            "status": status_value,
            "status_class": status_classes.get(status_value, "status-pill-neutral"),
            "owner": owner_value,
            "ecd": {
                "value": ecd_value,
                "editable": ecd_editable,
                "metric_key": ecd_key if (ecd_key and ecd_key in CLICKUP_FIELD_MAP) else "",
                "override_key": ecd_override_key,
                "input_value": us_to_ymd(ecd_value),
            },
            "acd": {
                "value": acd_value,
                "editable": acd_editable,
                "metric_key": acd_key,
                "input_value": us_to_ymd(acd_value),
            },
            "extras": extras,
        }
    return display


def compute_step_status(
    fields: dict[str, str],
    is_kickoff: bool,
) -> str:
    acd = parse_us_date(fields.get("ACD", ""))
    if acd:
        return "Completed" if is_kickoff else "On Track"

    if is_kickoff:
        return "Not Started"

    ecd = parse_us_date(fields.get("ECD", ""))
    if not ecd:
        return "Not Started"

    delta_days = (ecd.date() - date.today()).days
    if delta_days < 0:
        return "Roadblock/Overage"
    if delta_days <= 3:
        return "Potential Roadblock"
    return "On Track"


def add_ecd_acd_fields(
    steps: dict[str, dict[str, str]],
    field_keys: dict[str, dict[str, str]],
    step_slugs: dict[str, str],
    offsets: dict[str, int],
    ecd_overrides: dict[str, str],
    acd_anchor_preferences: dict[str, bool] | None = None,
) -> None:
    acd_anchor_preferences = acd_anchor_preferences or {}

    def find_step_title_by_slug(target_slug: str) -> str | None:
        for title, mapped_slug in step_slugs.items():
            if mapped_slug == target_slug:
                return title
        return None

    def anchor_date_for_slug(target_slug: str) -> datetime | None:
        title = find_step_title_by_slug(target_slug)
        if not title:
            return None
        fields = steps.get(title, {})
        acd = parse_us_date(fields.get("ACD", ""))
        ecd = parse_us_date(fields.get("ECD", ""))
        use_acd = acd_anchor_preferences.get(target_slug, True)
        if use_acd:
            return acd or ecd
        return ecd or acd

    def set_ecd_if_blank(step_slug: str, anchor_slug: str, days: int) -> None:
        step_title = find_step_title_by_slug(step_slug)
        anchor = anchor_date_for_slug(anchor_slug)
        if not step_title or not anchor:
            return
        fields = steps.setdefault(step_title, {})
        if fields.get("ECD"):
            return
        fields["ECD"] = format_us_date(shift_to_monday_if_weekend(anchor + timedelta(days=days)))
        field_keys.setdefault(step_title, {}).setdefault("ECD", "")

    def set_ecd_from_date_if_blank(step_slug: str, anchor: datetime | None, days: int) -> None:
        step_title = find_step_title_by_slug(step_slug)
        if not step_title or not anchor:
            return
        fields = steps.setdefault(step_title, {})
        if fields.get("ECD"):
            return
        fields["ECD"] = format_us_date(shift_to_monday_if_weekend(anchor + timedelta(days=days)))
        field_keys.setdefault(step_title, {}).setdefault("ECD", "")

    def shift_business_safe(value: datetime, delta_days: int) -> datetime:
        shifted = value + timedelta(days=delta_days)
        if delta_days < 0:
            return shift_to_friday_if_weekend(shifted)
        return shift_to_monday_if_weekend(shifted)

    kickoff_title = None
    kickoff_slug = None
    for step_title, slug in step_slugs.items():
        if "kickoff" in slug:
            kickoff_title = step_title
            kickoff_slug = slug
            break

    if kickoff_title:
        kickoff_fields = steps.get(kickoff_title, {})
        kickoff_fields["ECD"] = kickoff_fields.get("ACD", "")
        field_keys.setdefault(kickoff_title, {}).pop("ECD", None)
        kickoff_fields.setdefault("ACD", "")
        kickoff_fields["Status"] = compute_step_status(kickoff_fields, is_kickoff=True)
        ordered = {
            "Status": kickoff_fields.get("Status", ""),
            "ECD": kickoff_fields.get("ECD", ""),
            "ACD": kickoff_fields.get("ACD", ""),
        }
        ordered_keys = {
            "ECD": field_keys.get(kickoff_title, {}).get("ECD", ""),
            "ACD": field_keys.get(kickoff_title, {}).get("ACD", ""),
        }
        for key, value in kickoff_fields.items():
            if key not in {"Status", "ECD", "ACD"}:
                ordered[key] = value
                ordered_keys[key] = field_keys.get(kickoff_title, {}).get(key, "")
        steps[kickoff_title] = ordered
        field_keys[kickoff_title] = ordered_keys

    # Explicit SRA rules to avoid unintended cross-step drift:
    # - Receive Policies... ECD = Kickoff anchor + 7 days
    # - Review Policies... ECD = Receive Policies... anchor + 12 days
    # - Schedule Onsite/Remote Interview ECD = Kickoff anchor + 14 days
    # - Go Onsite/Have Interviews:
    #   - if ACD set, ECD mirrors ACD
    #   - else ECD = Review Policies... anchor + 7 days
    # - Receive Requested Follow Up Documentation ECD = Go Onsite ECD + 14 days
    # - Schedule Final SRA Report ECD = Go Onsite anchor + 14 days
    # - Review SRA:
    #   - if Present Final SRA Report ACD not set => Go Onsite anchor + 15 days
    #   - else => Present Final SRA Report ACD - 1 day
    # - Present Final SRA Report:
    #   - if ACD set, ECD mirrors ACD
    #   - else ECD = Review SRA anchor + 7 days
    set_ecd_if_blank("receive_policies_and_procedures_baa", "sra_kickoff", 7)
    set_ecd_if_blank("review_policies_and_procedures_baa", "receive_policies_and_procedures_baa", 12)
    set_ecd_if_blank("schedule_onsite_remote_interview", "sra_kickoff", 14)

    go_slug = "go_onsite_have_interview"
    go_title = find_step_title_by_slug(go_slug)
    if go_title:
        go_fields = steps.setdefault(go_title, {})
        go_acd = parse_us_date(go_fields.get("ACD", ""))
        if go_acd:
            go_fields["ECD"] = format_us_date(go_acd)
        else:
            set_ecd_if_blank(go_slug, "review_policies_and_procedures_baa", 7)

    set_ecd_if_blank(
        "recieve_requested_follow_up_documentation",
        "go_onsite_have_interview",
        14,
    )
    set_ecd_if_blank("schedule_final_sra_report", "go_onsite_have_interview", 14)

    review_sra_slug = "review_sra"
    review_sra_title = find_step_title_by_slug(review_sra_slug)
    if review_sra_title:
        review_sra_fields = steps.setdefault(review_sra_title, {})
        if not review_sra_fields.get("ECD"):
            present_final_acd = None
            present_final_title = find_step_title_by_slug("present_final_sra_report")
            if present_final_title:
                present_final_fields = steps.get(present_final_title, {})
                present_final_acd = parse_us_date(present_final_fields.get("ACD", ""))

            if present_final_acd:
                review_sra_fields["ECD"] = format_us_date(
                    shift_to_friday_if_weekend(present_final_acd - timedelta(days=1))
                )
            else:
                go_anchor = anchor_date_for_slug("go_onsite_have_interview")
                if go_anchor:
                    proposed_review = shift_to_monday_if_weekend(go_anchor + timedelta(days=15))

                    receive_title = find_step_title_by_slug("recieve_requested_follow_up_documentation")
                    schedule_title = find_step_title_by_slug("schedule_final_sra_report")
                    sibling_dates: list[datetime] = []
                    if receive_title:
                        d = parse_us_date(steps.get(receive_title, {}).get("ECD", ""))
                        if d:
                            sibling_dates.append(d)
                    if schedule_title:
                        d = parse_us_date(steps.get(schedule_title, {}).get("ECD", ""))
                        if d:
                            sibling_dates.append(d)

                    if sibling_dates:
                        latest_sibling = max(sibling_dates)
                        # If weekend shifting collapses Review SRA onto sibling dates,
                        # keep Review SRA at least one business day later.
                        if proposed_review <= latest_sibling:
                            proposed_review = next_business_day(latest_sibling)

                    review_sra_fields["ECD"] = format_us_date(proposed_review)
                else:
                    review_sra_fields["ECD"] = ""
            field_keys.setdefault(review_sra_title, {}).setdefault("ECD", "")

    present_slug = "present_final_sra_report"
    present_title = find_step_title_by_slug(present_slug)
    if present_title:
        present_fields = steps.setdefault(present_title, {})
        if not present_fields.get("ECD"):
            present_acd = parse_us_date(present_fields.get("ACD", ""))
            if present_acd:
                present_fields["ECD"] = format_us_date(present_acd)
            else:
                review_title = find_step_title_by_slug("review_sra")
                review_fields = steps.get(review_title, {}) if review_title else {}
                review_ecd = parse_us_date(review_fields.get("ECD", ""))
                review_acd = parse_us_date(review_fields.get("ACD", ""))

                # Anchor on Review SRA ECD by default; only use Review SRA ACD
                # if ACD is later than ECD (true delay scenario).
                review_anchor = review_ecd
                if review_acd and (not review_ecd or review_acd > review_ecd):
                    review_anchor = review_acd
                if review_anchor:
                    candidate = shift_to_monday_if_weekend(review_anchor + timedelta(days=7))

                    # Guardrail: Present Final SRA should not be due before
                    # prerequisite SRA boxes that come before it.
                    prerequisite_slugs = [
                        "recieve_requested_follow_up_documentation",
                        "schedule_final_sra_report",
                        "review_sra",
                    ]
                    prerequisite_dates: list[datetime] = []
                    for prereq_slug in prerequisite_slugs:
                        prereq_date = anchor_date_for_slug(prereq_slug)
                        if prereq_date:
                            prerequisite_dates.append(prereq_date)
                    if prerequisite_dates:
                        min_allowed = max(prerequisite_dates)
                        if candidate <= min_allowed:
                            candidate = next_business_day(min_allowed)

                    present_fields["ECD"] = format_us_date(candidate)
                else:
                    present_fields["ECD"] = ""
            field_keys.setdefault(present_title, {}).setdefault("ECD", "")

    # Explicit NVA rules:
    # - Receive Credentials ECD = NVA Kickoff anchor + 7 days
    # - Verify Access ECD = Receive Credentials anchor + 7 days
    # - Scans Complete ECD:
    #   - if Receive Credentials ACD or Verify Access ACD is blank => NVA Kickoff anchor + 28 days
    #   - else => max(Receive Credentials ACD, Verify Access ACD) + 21 days
    # - Compile Report ECD:
    #   - if Present Final NVA Report ACD is blank => Scans Complete ECD + 7 days
    #   - else => Present Final NVA Report ACD - 1 day
    # - Access Removed ECD:
    #   - if Present Final NVA Report ACD is blank => Scans Complete ECD + 5 days
    #   - else => Present Final NVA Report ACD - 1 day
    # - Schedule Final NVA Report ECD:
    #   - if Scans Complete ACD is blank => Scans Complete ECD + 12 days
    #   - else => Scans Complete ACD + 21 days
    # - Present Final NVA Report ECD:
    #   - if ACD is blank => Scans Complete ECD + 19 days
    #   - else => mirror ACD
    set_ecd_if_blank("receive_credentials", "nva_kickoff", 7)
    set_ecd_if_blank("verify_access", "receive_credentials", 7)

    scans_slug = "scans_complete"
    scans_title = find_step_title_by_slug(scans_slug)
    if scans_title:
        scans_fields = steps.setdefault(scans_title, {})
        if not scans_fields.get("ECD"):
            receive_title = find_step_title_by_slug("receive_credentials")
            verify_title = find_step_title_by_slug("verify_access")
            receive_acd = parse_us_date(steps.get(receive_title, {}).get("ACD", "")) if receive_title else None
            verify_acd = parse_us_date(steps.get(verify_title, {}).get("ACD", "")) if verify_title else None

            if not receive_acd or not verify_acd:
                kickoff_anchor = anchor_date_for_slug("nva_kickoff")
                set_ecd_from_date_if_blank(scans_slug, kickoff_anchor, 28)
            else:
                set_ecd_from_date_if_blank(scans_slug, max(receive_acd, verify_acd), 21)

    scans_anchor_title = find_step_title_by_slug("scans_complete")
    scans_fields = steps.get(scans_anchor_title, {}) if scans_anchor_title else {}
    scans_ecd = parse_us_date(scans_fields.get("ECD", ""))
    scans_acd = parse_us_date(scans_fields.get("ACD", ""))

    present_nva_slug = "present_final_nva_report"
    present_nva_title = find_step_title_by_slug(present_nva_slug)
    present_nva_fields = steps.setdefault(present_nva_title, {}) if present_nva_title else {}
    present_nva_acd = parse_us_date(present_nva_fields.get("ACD", "")) if present_nva_title else None

    compile_slug = "compile_report"
    compile_title = find_step_title_by_slug(compile_slug)
    if compile_title:
        compile_fields = steps.setdefault(compile_title, {})
        if present_nva_acd:
            compile_fields["ECD"] = format_us_date(
                shift_to_friday_if_weekend(present_nva_acd - timedelta(days=1))
            )
            field_keys.setdefault(compile_title, {}).setdefault("ECD", "")
        else:
            set_ecd_from_date_if_blank(compile_slug, scans_ecd, 7)

    access_removed_slug = "access_removed"
    access_removed_title = find_step_title_by_slug(access_removed_slug)
    if access_removed_title:
        access_removed_fields = steps.setdefault(access_removed_title, {})
        if present_nva_acd:
            access_removed_fields["ECD"] = format_us_date(
                shift_to_friday_if_weekend(present_nva_acd - timedelta(days=1))
            )
            field_keys.setdefault(access_removed_title, {}).setdefault("ECD", "")
        else:
            set_ecd_from_date_if_blank(access_removed_slug, scans_ecd, 5)

    schedule_nva_slug = "schedule_final_nva_report"
    schedule_nva_title = find_step_title_by_slug(schedule_nva_slug)
    if schedule_nva_title:
        if scans_acd:
            set_ecd_from_date_if_blank(schedule_nva_slug, scans_acd, 21)
        else:
            set_ecd_from_date_if_blank(schedule_nva_slug, scans_ecd, 12)

    if present_nva_title:
        if present_nva_acd:
            present_nva_fields["ECD"] = format_us_date(present_nva_acd)
            field_keys.setdefault(present_nva_title, {}).setdefault("ECD", "")
        else:
            set_ecd_from_date_if_blank(present_nva_slug, scans_ecd, 19)

    # Fallback for all other steps: section kickoff anchor + offset.
    for slug, offset_days in offsets.items():
        if slug in {
            "receive_policies_and_procedures_baa",
            "review_policies_and_procedures_baa",
            "schedule_onsite_remote_interview",
            "go_onsite_have_interview",
            "recieve_requested_follow_up_documentation",
            "review_sra",
            "schedule_final_sra_report",
            "present_final_sra_report",
            "sra_kickoff",
            "receive_credentials",
            "verify_access",
            "scans_complete",
            "access_removed",
            "compile_report",
            "schedule_final_nva_report",
            "present_final_nva_report",
            "nva_kickoff",
        }:
            continue
        if kickoff_slug:
            set_ecd_if_blank(slug, kickoff_slug, offset_days)

    # Apply manual ECD overrides and propagate day-delta across later steps in the same track.
    ordered_slugs: list[str] = []
    if kickoff_slug:
        ordered_slugs.append(kickoff_slug)
    for slug in offsets.keys():
        if slug in step_slugs.values() and slug not in ordered_slugs:
            ordered_slugs.append(slug)
    for slug in step_slugs.values():
        if slug not in ordered_slugs:
            ordered_slugs.append(slug)

    for index, slug in enumerate(ordered_slugs):
        override_value = str(ecd_overrides.get(slug, "")).strip()
        if not override_value:
            continue
        override_dt = parse_us_date(override_value)
        if not override_dt:
            continue
        step_title = find_step_title_by_slug(slug)
        if not step_title:
            continue

        fields = steps.setdefault(step_title, {})
        current_ecd_dt = parse_us_date(fields.get("ECD", ""))
        delta_days = (
            (override_dt.date() - current_ecd_dt.date()).days if current_ecd_dt else 0
        )
        fields["ECD"] = format_us_date(override_dt)
        field_keys.setdefault(step_title, {})["ECD_OVERRIDE"] = f"override:{slug}.ecd"

        if delta_days == 0:
            continue

        for later_slug in ordered_slugs[index + 1 :]:
            later_title = find_step_title_by_slug(later_slug)
            if not later_title:
                continue
            later_fields = steps.setdefault(later_title, {})

            # If ACD exists, treat step as complete and do not shift it.
            if parse_us_date(later_fields.get("ACD", "")):
                continue

            later_ecd_dt = parse_us_date(later_fields.get("ECD", ""))
            if not later_ecd_dt:
                continue
            later_fields["ECD"] = format_us_date(
                shift_business_safe(later_ecd_dt, delta_days)
            )

    for step_title, fields in list(steps.items()):
        slug = step_slugs.get(step_title, "")
        is_kickoff = "kickoff" in slug

        fields.setdefault("ACD", "")
        field_keys.setdefault(step_title, {}).setdefault("ACD", "")
        fields.setdefault("ECD", "")
        field_keys.setdefault(step_title, {}).setdefault("ECD", "")
        if not is_kickoff:
            field_keys.setdefault(step_title, {}).setdefault("ECD_OVERRIDE", f"override:{slug}.ecd")

        fields["Status"] = compute_step_status(fields, is_kickoff=is_kickoff)
        ordered = {
            "Status": fields.get("Status", ""),
            "ECD": fields.get("ECD", ""),
            "ACD": fields.get("ACD", ""),
        }
        ordered_keys = {
            "ECD": field_keys.get(step_title, {}).get("ECD", ""),
            "ACD": field_keys.get(step_title, {}).get("ACD", ""),
        }
        if not is_kickoff and field_keys.get(step_title, {}).get("ECD_OVERRIDE", ""):
            ordered_keys["ECD_OVERRIDE"] = field_keys.get(step_title, {}).get(
                "ECD_OVERRIDE", ""
            )
        for key, value in fields.items():
            if key not in {"Status", "ECD", "ACD"}:
                ordered[key] = value
                ordered_keys[key] = field_keys.get(step_title, {}).get(key, "")
        steps[step_title] = ordered
        field_keys[step_title] = ordered_keys


def build_dashboard_view(
    status: dict,
    ecd_overrides: dict[str, str],
    can_edit: bool,
    acd_anchor_preferences: dict[str, bool] | None = None,
) -> dict:
    metrics = status.get("metrics", {}) or {}
    sra_toggle = parse_bool(
        metrics.get("project.sra_enabled")
        or metrics.get("sra.enabled")
        or metrics.get("sra_enabled")
    )
    nva_toggle = parse_bool(
        metrics.get("project.nva_enabled")
        or metrics.get("nva.enabled")
        or metrics.get("nva_enabled")
    )
    location_value = str(
        metrics.get("project.remote_onsite")
        or metrics.get("project.location")
        or ""
    ).strip()

    project_values: dict[str, str] = {}
    sra_steps: dict[str, dict[str, str]] = {}
    nva_steps: dict[str, dict[str, str]] = {}
    sra_field_keys: dict[str, dict[str, str]] = {}
    nva_field_keys: dict[str, dict[str, str]] = {}
    sra_step_slugs: dict[str, str] = {}
    nva_step_slugs: dict[str, str] = {}
    extra_metrics: dict[str, str] = {}

    for key, value in metrics.items():
        parts = str(key).split(".")
        if len(parts) < 2:
            extra_metrics[key] = value
            continue

        section = parts[0].strip().lower()
        if section == "project":
            field_slug = parts[1].strip().lower()
            if field_slug not in {"sra_enabled", "nva_enabled"}:
                field = project_field_label(field_slug)
                project_values[field] = value
            continue

        if len(parts) < 3:
            extra_metrics[key] = value
            continue

        step_slug = parts[1].strip().lower()
        step_name = step_title(section, step_slug, location_value)
        field_slug = parts[2].strip().lower()
        if field_slug == "date":
            field_name = "ACD"
        elif field_slug in {"acd", "ecd"}:
            field_name = field_slug.upper()
        else:
            field_name = apply_acronyms(to_title_case(field_slug))

        if section == "sra":
            sra_step_slugs.setdefault(step_name, step_slug)
            sra_steps.setdefault(step_name, {})[field_name] = value
            sra_field_keys.setdefault(step_name, {})[field_name] = key
        elif section == "nva":
            nva_step_slugs.setdefault(step_name, step_slug)
            nva_steps.setdefault(step_name, {})[field_name] = value
            nva_field_keys.setdefault(step_name, {})[field_name] = key
        else:
            extra_metrics[key] = value

    # If both sections are enabled on the same task, mirror SRA kickoff date to NVA kickoff.
    if sra_toggle is True and nva_toggle is True:
        sra_kickoff_title = next(
            (title for title, slug in sra_step_slugs.items() if slug == "sra_kickoff"),
            None,
        )
        nva_kickoff_title = next(
            (title for title, slug in nva_step_slugs.items() if slug == "nva_kickoff"),
            None,
        )
        if sra_kickoff_title and nva_kickoff_title:
            sra_kickoff_acd = str(
                sra_steps.get(sra_kickoff_title, {}).get("ACD", "")
            ).strip()
            if sra_kickoff_acd:
                nva_steps.setdefault(nva_kickoff_title, {})["ACD"] = sra_kickoff_acd

    add_ecd_acd_fields(
        sra_steps,
        sra_field_keys,
        sra_step_slugs,
        SRA_ECD_OFFSETS_DAYS,
        ecd_overrides,
        acd_anchor_preferences=acd_anchor_preferences,
    )
    add_ecd_acd_fields(
        nva_steps,
        nva_field_keys,
        nva_step_slugs,
        NVA_ECD_OFFSETS_DAYS,
        ecd_overrides,
        acd_anchor_preferences=acd_anchor_preferences,
    )

    # Keep Present Final NVA Report ECD aligned to Present Final SRA Report ECD.
    sra_present_title = next(
        (title for title, slug in sra_step_slugs.items() if slug == "present_final_sra_report"),
        None,
    )
    nva_present_title = next(
        (title for title, slug in nva_step_slugs.items() if slug == "present_final_nva_report"),
        None,
    )
    if sra_present_title and nva_present_title:
        sra_present_ecd = str(sra_steps.get(sra_present_title, {}).get("ECD", "")).strip()
        if sra_present_ecd:
            nva_present_fields = nva_steps.setdefault(nva_present_title, {})
            nva_present_fields["ECD"] = sra_present_ecd
            nva_present_fields["Status"] = compute_step_status(
                nva_present_fields, is_kickoff=False
            )
            nva_field_keys.setdefault(nva_present_title, {}).setdefault("ECD", "")

    default_owner = str(status.get("task_name", "")).strip() or "Not assigned"
    for step_name, step_fields in sra_steps.items():
        slug = sra_step_slugs.get(step_name, "")
        step_fields["Owner"] = step_owner("sra", slug, default_owner)
    for step_name, step_fields in nva_steps.items():
        slug = nva_step_slugs.get(step_name, "")
        step_fields["Owner"] = step_owner("nva", slug, default_owner)

    if not str(project_values.get("Project Lead", "")).strip():
        project_values["Project Lead"] = "Not assigned"
    if not str(project_values.get("Project Support", "")).strip():
        project_values.pop("Project Support", None)

    # Explicit display ordering and filtering for client-facing project details.
    project_details: dict[str, str] = {
        "Status": apply_acronyms(to_title_case(status.get("task_status", ""))),
    }
    for key in ["Project Lead", "Project Support", "Location", "Next Steps"]:
        value = str(project_values.get(key, "")).strip()
        if key == "Next Steps":
            project_details[key] = value or "Not set"
        elif value:
            project_details[key] = value

    # Strict visibility: only show a section when its explicit enable checkbox is true.
    show_sra = sra_toggle is True
    show_nva = nva_toggle is True

    return {
        "project_details": project_details,
        "show_sra": show_sra,
        "show_nva": show_nva,
        "sra_steps": build_step_display(
            sra_steps, sra_field_keys, sra_step_slugs, can_edit=can_edit
        ),
        "nva_steps": build_step_display(
            nva_steps, nva_field_keys, nva_step_slugs, can_edit=can_edit
        ),
        "extra_metrics": extra_metrics,
    }


def require_admin() -> None:
    if not ADMIN_API_KEY:
        return
    provided = request.headers.get("X-API-Key", "")
    if not hmac.compare_digest(provided, ADMIN_API_KEY):
        abort(401)


def require_admin_page() -> None:
    if not ADMIN_API_KEY:
        return
    provided = request.args.get("key", "") or request.headers.get("X-API-Key", "")
    if not hmac.compare_digest(provided, ADMIN_API_KEY):
        abort(401)


def has_admin_key_access() -> bool:
    if not ADMIN_API_KEY:
        return True
    provided = request.args.get("key", "") or request.headers.get("X-API-Key", "")
    return hmac.compare_digest(provided, ADMIN_API_KEY)


def has_admin_edit_access() -> bool:
    return has_admin_key_access() and request.args.get("mode") == "admin"


def sign_sf_id(sf_id: str) -> str:
    return hmac.new(
        CLIENT_LINK_SECRET.encode("utf-8"),
        sf_id.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def valid_client_signature(sf_id: str, sig: str) -> bool:
    if not sig:
        return False
    return hmac.compare_digest(sign_sf_id(sf_id), sig)


@app.get("/")
def home():
    return jsonify(
        {
            "service": "medcurity-client-dashboard",
            "endpoints": {
                "generate_link": "/admin/generate-link/<sf_id>",
                "manual_sync": "/admin/sync",
                "admin_projects": "/admin/projects?key=<ADMIN_API_KEY>",
                "status_page": "/status/<sf_id>?sig=<signature>",
                "update_date": "/status/<sf_id>/update-date?sig=<signature>",
                "clickup_webhook": "/webhook/clickup",
            },
        }
    )


@app.get("/admin/generate-link/<sf_id>")
def generate_link(sf_id: str):
    require_admin()
    return jsonify(
        {
            "sf_id": sf_id,
            "signature": sign_sf_id(sf_id),
            "url_path": f"/status/{sf_id}?sig={sign_sf_id(sf_id)}",
        }
    )


@app.post("/admin/sync")
def manual_sync():
    require_admin()
    total = refresh_all_from_clickup()
    return jsonify({"ok": True, "synced_clients": total})


@app.post("/webhook/clickup")
def clickup_webhook():
    if WEBHOOK_TOKEN:
        provided = request.args.get("token", "")
        if not hmac.compare_digest(provided, WEBHOOK_TOKEN):
            abort(401)

    payload = request.get_json(silent=True) or {}

    # ClickUp webhook payloads can vary by event type.
    task_id = str(payload.get("task_id") or "").strip()
    if not task_id:
        task = payload.get("task")
        if isinstance(task, dict):
            task_id = str(task.get("id") or "").strip()

    if not task_id:
        return jsonify({"ok": True, "ignored": "missing_task_id"})

    full_task = fetch_task_by_id(CLICKUP_API_TOKEN, task_id)
    normalized = normalize_task(full_task, CLICKUP_SF_ID_FIELD_ID, CLICKUP_FIELD_MAP)

    if not normalized:
        return jsonify({"ok": True, "ignored": "missing_sf_id"})
    normalized["metrics"]["project.next_steps"] = fetch_latest_task_comment(
        CLICKUP_API_TOKEN, normalized["task_id"]
    )

    upsert_client_status(DATABASE_PATH, strip_internal_fields(normalized))
    return jsonify({"ok": True, "updated_sf_id": normalized["sf_id"]})


@app.get("/admin/projects")
def admin_projects():
    require_admin_page()
    # Optional live refresh, disabled by default for faster navigation.
    if request.args.get("refresh") == "1":
        try:
            recent_count = refresh_recent_from_clickup()
            if recent_count == 0:
                refresh_all_from_clickup()
        except Exception:
            # If ClickUp is temporarily unavailable, fall back to cached DB data.
            pass

    rows = list_client_statuses(DATABASE_PATH)
    admin_key = request.args.get("key", "")
    projects = []
    for row in rows:
        status_clean = apply_acronyms(to_title_case(row["task_status"]))
        is_completed = status_clean.lower() == "completed"

        # Better bucketing anchors:
        # - Active: created date
        # - Completed: final milestone date first, then closed date fallback
        anchor_dt = datetime.min
        metrics = row.get("metrics", {}) or {}
        if is_completed:
            sra_final = parse_metric_us_date(metrics.get("sra.present_final_sra_report.date", ""))
            nva_final = parse_metric_us_date(metrics.get("nva.present_final_nva_report.date", ""))
            milestone_anchor = max(sra_final, nva_final)
            if milestone_anchor != datetime.min:
                anchor_dt = milestone_anchor
            else:
                anchor_dt = parse_iso_or_min(row.get("task_closed_at", ""))
                if anchor_dt == datetime.min:
                    anchor_dt = parse_iso_or_min(row.get("source_updated_at", ""))
        else:
            anchor_dt = parse_iso_or_min(row.get("task_created_at", ""))
            if anchor_dt == datetime.min:
                anchor_dt = parse_iso_or_min(row.get("source_updated_at", ""))

        start_dt = parse_iso_or_min(row.get("task_created_at", ""))
        completed_dt = anchor_dt if is_completed else datetime.min

        projects.append(
            {
                "task_name": row["task_name"],
                "sf_id": row["sf_id"],
                "project_lead": str(metrics.get("project.project_lead", "")).strip() or "Not assigned",
                "task_status": status_clean,
                "status_url": (
                    f"/status/{row['sf_id']}?sig={sign_sf_id(row['sf_id'])}"
                    f"&mode=admin&key={admin_key}"
                ),
                "start_date_label": format_dt_to_us(start_dt) if not is_completed else "-",
                "completed_date_label": format_dt_to_us(completed_dt) if is_completed else "-",
                "period_label": year_quarter_label_from_dt(anchor_dt),
            }
        )

    completed = [p for p in projects if p["task_status"].lower() == "completed"]
    active = [p for p in projects if p["task_status"].lower() != "completed"]

    # Temporary business rule: re-bucket completed 2026 Q1 work to 2025 Q4.
    for project in completed:
        if project["task_name"].strip().lower() == "acs":
            project["period_label"] = "2026 Q1"
            project["completed_date_label"] = "01/14/2026"
        elif project["period_label"] == "2026 Q1":
            project["period_label"] = "2025 Q4"
            project["completed_date_label"] = "12/31/2025"

    # Temporary business rule: no active projects in 2025 Q4.
    # Move any remaining active 2025 Q4 projects into 2026 Q1.
    for project in active:
        name_key = project["task_name"].strip().lower()
        if "overlake arthritis" in name_key or name_key == "the rose":
            project["period_label"] = "2026 Q1"
            project["start_date_label"] = "01/01/2026"
            continue

        if project["period_label"] == "2025 Q4" and "yale" not in name_key:
            project["period_label"] = "2026 Q1"
            project["start_date_label"] = "01/01/2026"

    def status_counts(items: list[dict]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in items:
            key = item["task_status"] or "Unknown"
            counts[key] = counts.get(key, 0) + 1
        return dict(sorted(counts.items(), key=lambda kv: kv[0].lower()))

    active_status_counts = status_counts(active)
    completed_status_counts = status_counts(completed)
    all_statuses = sorted(
        set(active_status_counts) | set(completed_status_counts),
        key=lambda value: value.lower(),
    )
    all_project_leads = sorted(
        {str(project.get("project_lead", "")).strip() or "Not assigned" for project in projects},
        key=lambda value: value.lower(),
    )

    return render_template(
        "admin_projects.html",
        completed_groups=group_projects_for_admin(completed),
        active_groups=group_projects_for_admin(active),
        count=len(projects),
        completed_count=len(completed),
        active_count=len(active),
        completed_status_counts=completed_status_counts,
        active_status_counts=active_status_counts,
        all_statuses=all_statuses,
        all_project_leads=all_project_leads,
        projects_url=f"/admin/projects?key={admin_key}",
        metrics_url=f"/admin/metrics?key={admin_key}",
    )


@app.get("/admin/metrics")
def admin_metrics():
    require_admin_page()
    admin_key = request.args.get("key", "")
    historical_rows = list_historical_close_metrics(DATABASE_PATH)
    historical_companies = {
        str(row.get("company", "")).strip().lower()
        for row in historical_rows
        if str(row.get("company", "")).strip()
    }
    historical_sf_ids = {
        str(row.get("sf_id", "")).strip()
        for row in historical_rows
        if str(row.get("sf_id", "")).strip()
    }
    live_rows, live_quality = live_close_metric_rows(list_client_statuses(DATABASE_PATH))
    covered_name_set: set[str] = set()
    uncovered_name_set: set[str] = set()
    for record in live_quality.get("missing_records", []):
        name = str(record.get("company", "")).strip()
        sf_id = str(record.get("sf_id", "")).strip()
        if not name and not sf_id:
            continue
        name_l = name.lower()
        name_alias = name_l.replace("(remote)", "").replace("(renewal)", "").strip()
        is_covered = (
            (sf_id and sf_id in historical_sf_ids)
            or (name_l in historical_companies)
            or (name_alias in historical_companies)
        )
        if is_covered:
            covered_name_set.add(name)
        else:
            uncovered_name_set.add(name)
    covered_missing = sorted(covered_name_set)
    uncovered_missing = sorted(uncovered_name_set)
    live_quality["missing_covered_by_historical"] = len(covered_missing)
    live_quality["missing_uncovered"] = max(
        live_quality.get("completed_task_missing_close_dates", 0) - len(covered_missing),
        0,
    )
    live_quality["missing_covered_names"] = covered_missing
    live_quality["missing_uncovered_names"] = uncovered_missing
    historical_keys = {
        "|".join(
            [
                str(r.get("company", "")).strip().lower(),
                str(r.get("track", "")).strip().upper(),
                str(r.get("kickoff_date", "")).strip(),
                str(r.get("final_date", "")).strip(),
            ]
        )
        for r in historical_rows
    }
    live_rows_deduped = [
        r
        for r in live_rows
        if "|".join(
            [
                str(r.get("company", "")).strip().lower(),
                str(r.get("track", "")).strip().upper(),
                str(r.get("kickoff_date", "")).strip(),
                str(r.get("final_date", "")).strip(),
            ]
        )
        not in historical_keys
    ]
    all_rows = historical_rows + live_rows_deduped
    all_rows = sorted(
        all_rows,
        key=lambda row: parse_any_us_date(row.get("final_date", "")) or datetime.min,
        reverse=True,
    )
    quarter_rows, summary = summarize_metrics_rows(all_rows)
    historical_quarters = sorted({row.get("quarter_label", "") for row in historical_rows if row.get("quarter_label")})
    historical_range = (
        f"{historical_quarters[0]} to {historical_quarters[-1]}"
        if historical_quarters
        else "No historical rows loaded"
    )
    all_quarters = sorted({row["quarter_label"] for row in all_rows})
    all_tracks = sorted({row["track"] for row in all_rows})
    all_sources = sorted({row.get("source", "") for row in all_rows})
    coverage_pct = (
        round(
            100
            * live_quality["completed_task_with_valid_close"]
            / live_quality["completed_task_total"],
            1,
        )
        if live_quality["completed_task_total"]
        else None
    )
    return render_template(
        "admin_metrics.html",
        summary=summary,
        quarter_rows=quarter_rows,
        metric_rows=all_rows,
        all_quarters=all_quarters,
        all_tracks=all_tracks,
        all_sources=all_sources,
        coverage_pct=coverage_pct,
        live_quality=live_quality,
        historical_range=historical_range,
        total_rows=len(all_rows),
        historical_count=len(historical_rows),
        live_count=len(live_rows_deduped),
        projects_url=f"/admin/projects?key={admin_key}",
        metrics_url=f"/admin/metrics?key={admin_key}",
        key=admin_key,
    )


@app.post("/admin/metrics/import")
def admin_metrics_import():
    require_admin_page()
    payload = str(request.form.get("historical_tsv", ""))
    parsed = parse_historical_tsv(payload)
    saved = upsert_historical_close_metrics(DATABASE_PATH, parsed) if parsed else 0
    admin_key = request.args.get("key", "")
    return redirect(url_for("admin_metrics", key=admin_key, imported=saved))


@app.post("/status/<sf_id>/update-date")
def update_date(sf_id: str):
    signature = request.args.get("sig", "")
    if not valid_client_signature(sf_id, signature):
        abort(403)
    if not has_admin_edit_access():
        abort(403)

    metric_key = str(request.form.get("metric_key", "")).strip()
    override_key = str(request.form.get("override_key", "")).strip()
    step_slug = str(request.form.get("step_slug", "")).strip().lower()
    adjust_following = str(request.form.get("adjust_following", "yes")).strip().lower()
    value_ymd = str(request.form.get("value", "")).strip()
    if not metric_key and not override_key:
        abort(400)

    status = get_client_status(DATABASE_PATH, sf_id)
    if not status:
        abort(404)
    old_value = ""

    if metric_key and metric_key in CLICKUP_FIELD_MAP:
        old_value = str((status.get("metrics", {}) or {}).get(metric_key, ""))
        field_id = CLICKUP_FIELD_MAP[metric_key]
        clickup_value = ymd_to_clickup_ms(value_ymd) if value_ymd else None
        set_task_custom_field_value(CLICKUP_API_TOKEN, status["task_id"], field_id, clickup_value)
        if step_slug and (metric_key.endswith(".date") or metric_key.endswith(".acd")):
            upsert_acd_anchor_preference(
                DATABASE_PATH,
                sf_id,
                step_slug,
                use_acd=(adjust_following != "no"),
            )

        full_task = fetch_task_by_id(CLICKUP_API_TOKEN, status["task_id"])
        normalized = normalize_task(full_task, CLICKUP_SF_ID_FIELD_ID, CLICKUP_FIELD_MAP)
        if normalized:
            upsert_client_status(DATABASE_PATH, strip_internal_fields(normalized))
        logged_new = (
            datetime.strptime(value_ymd, "%Y-%m-%d").strftime("%m/%d/%Y")
            if value_ymd
            else ""
        )
        log_edit(
            DATABASE_PATH,
            sf_id=sf_id,
            task_id=status.get("task_id", ""),
            field_key=metric_key,
            old_value=old_value,
            new_value=logged_new,
            source="admin_update_clickup",
        )
    elif override_key.startswith("override:") and override_key.endswith(".ecd"):
        step_slug = override_key[len("override:") : -len(".ecd")]
        old_value = get_ecd_overrides(DATABASE_PATH, sf_id).get(step_slug, "")
        logged_new = (
            datetime.strptime(value_ymd, "%Y-%m-%d").strftime("%m/%d/%Y")
            if value_ymd
            else ""
        )
        upsert_ecd_override(DATABASE_PATH, sf_id, step_slug, logged_new)
        log_edit(
            DATABASE_PATH,
            sf_id=sf_id,
            task_id=status.get("task_id", ""),
            field_key=override_key,
            old_value=old_value,
            new_value=logged_new,
            source="admin_update_override",
        )
    else:
        abort(400)

    return redirect(
        url_for(
            "client_status",
            sf_id=sf_id,
            sig=signature,
            mode=request.args.get("mode", ""),
            key=request.args.get("key", ""),
        )
    )


@app.get("/status/<sf_id>")
def client_status(sf_id: str):
    signature = request.args.get("sig", "")
    if not valid_client_signature(sf_id, signature):
        abort(403)

    # Keep client page current with ClickUp on browser refresh.
    try:
        refresh_client_from_clickup(sf_id)
    except Exception:
        # Fall back to cached DB data if ClickUp is temporarily unavailable.
        pass

    status = get_client_status(DATABASE_PATH, sf_id)
    if not status:
        abort(404)

    can_edit = has_admin_edit_access()
    can_admin_nav = has_admin_key_access()
    admin_key = request.args.get("key", "")
    overrides = get_ecd_overrides(DATABASE_PATH, sf_id)
    acd_anchor_preferences = get_acd_anchor_preferences(DATABASE_PATH, sf_id)
    dashboard = build_dashboard_view(
        status,
        ecd_overrides=overrides,
        can_edit=can_edit,
        acd_anchor_preferences=acd_anchor_preferences,
    )
    return render_template(
        "status.html",
        status=status,
        dashboard=dashboard,
        signature=signature,
        can_edit=can_edit,
        can_admin_nav=can_admin_nav,
        admin_dashboard_url=f"/admin/projects?key={admin_key}" if can_admin_nav and admin_key else "",
        client_view_url=(
            f"/status/{sf_id}?sig={signature}&preview=1&key={admin_key}"
            if can_edit and admin_key
            else f"/status/{sf_id}?sig={signature}"
        ),
        admin_edit_url=(
            f"/status/{sf_id}?sig={signature}&mode=admin&key={admin_key}"
            if can_admin_nav and admin_key
            else ""
        ),
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=True)
