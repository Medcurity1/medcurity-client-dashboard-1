import hashlib
import hmac
from datetime import date, datetime, timedelta

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
    get_ecd_overrides,
    init_db,
    list_client_statuses,
    log_edit,
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


def is_better_row(candidate: dict, current: dict) -> bool:
    if candidate.get("task_closed") != current.get("task_closed"):
        return not candidate.get("task_closed")
    return parse_iso_or_min(candidate.get("source_updated_at", "")) >= parse_iso_or_min(
        current.get("source_updated_at", "")
    )


def build_step_display(
    steps: dict[str, dict[str, str]],
    field_keys: dict[str, dict[str, str]],
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
        ecd_editable = can_edit
        acd_editable = can_edit and bool(acd_key) and acd_key in CLICKUP_FIELD_MAP

        extras: list[dict[str, str]] = []
        for label, value in fields.items():
            if label in {"Status", "Owner", "ECD", "ACD"}:
                continue
            extras.append({"label": label, "value": str(value or "")})

        display[step_name] = {
            "status": status_value,
            "status_class": status_classes.get(status_value, "status-pill-neutral"),
            "owner": owner_value,
            "ecd": {
                "value": ecd_value,
                "editable": ecd_editable,
                "metric_key": ecd_key if (ecd_key and ecd_key in CLICKUP_FIELD_MAP) else "",
                "override_key": field_keys.get(step_name, {}).get("ECD_OVERRIDE", ""),
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
) -> None:
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
        return parse_us_date(fields.get("ACD", "")) or parse_us_date(fields.get("ECD", ""))

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

    kickoff_title = None
    kickoff_date = None
    for step_title, slug in step_slugs.items():
        if "kickoff" in slug:
            kickoff_title = step_title
            kickoff_date = parse_us_date(steps.get(step_title, {}).get("ACD", ""))
            break

    if kickoff_title:
        kickoff_fields = steps.get(kickoff_title, {})
        kickoff_fields.pop("ECD", None)
        field_keys.setdefault(kickoff_title, {}).pop("ECD", None)
        kickoff_fields.setdefault("ACD", "")
        kickoff_fields["Status"] = compute_step_status(kickoff_fields, is_kickoff=True)
        ordered = {
            "Status": kickoff_fields.get("Status", ""),
            "ACD": kickoff_fields.get("ACD", ""),
        }
        ordered_keys = {"ACD": field_keys.get(kickoff_title, {}).get("ACD", "")}
        for key, value in kickoff_fields.items():
            if key not in {"Status", "ACD"}:
                ordered[key] = value
                ordered_keys[key] = field_keys.get(kickoff_title, {}).get(key, "")
        steps[kickoff_title] = ordered
        field_keys[kickoff_title] = ordered_keys

    # Apply manual ECD overrides early so downstream anchors can use them.
    for step_title, slug in step_slugs.items():
        override_value = ecd_overrides.get(slug, "")
        if not override_value:
            continue
        fields = steps.setdefault(step_title, {})
        fields["ECD"] = override_value
        field_keys.setdefault(step_title, {})["ECD_OVERRIDE"] = f"override:{slug}.ecd"

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

    # Fallback for all other steps: kickoff anchor + offset.
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
        }:
            continue
        set_ecd_if_blank(slug, "sra_kickoff", offset_days)

    for step_title, fields in list(steps.items()):
        slug = step_slugs.get(step_title, "")
        is_kickoff = "kickoff" in slug

        fields.setdefault("ACD", "")
        field_keys.setdefault(step_title, {}).setdefault("ACD", "")
        if not is_kickoff:
            fields.setdefault("ECD", "")
            field_keys.setdefault(step_title, {}).setdefault("ECD", "")
            field_keys.setdefault(step_title, {}).setdefault("ECD_OVERRIDE", f"override:{slug}.ecd")

        fields["Status"] = compute_step_status(fields, is_kickoff=is_kickoff)
        ordered = {"Status": fields.get("Status", ""), "ACD": fields.get("ACD", "")}
        if not is_kickoff:
            ordered["ECD"] = fields.get("ECD", "")
        ordered_keys = {
            "ACD": field_keys.get(step_title, {}).get("ACD", ""),
        }
        if not is_kickoff:
            ordered_keys["ECD"] = field_keys.get(step_title, {}).get("ECD", "")
            if field_keys.get(step_title, {}).get("ECD_OVERRIDE", ""):
                ordered_keys["ECD_OVERRIDE"] = field_keys.get(step_title, {}).get(
                    "ECD_OVERRIDE", ""
                )
        for key, value in fields.items():
            if key not in {"Status", "ECD", "ACD"}:
                ordered[key] = value
                ordered_keys[key] = field_keys.get(step_title, {}).get(key, "")
        steps[step_title] = ordered
        field_keys[step_title] = ordered_keys


def build_dashboard_view(status: dict, ecd_overrides: dict[str, str], can_edit: bool) -> dict:
    metrics = status.get("metrics", {}) or {}
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

    add_ecd_acd_fields(
        sra_steps,
        sra_field_keys,
        sra_step_slugs,
        SRA_ECD_OFFSETS_DAYS,
        ecd_overrides,
    )
    add_ecd_acd_fields(
        nva_steps,
        nva_field_keys,
        nva_step_slugs,
        NVA_ECD_OFFSETS_DAYS,
        ecd_overrides,
    )

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
    for key in ["Project Lead", "Location", "Next Steps", "Project Support"]:
        value = str(project_values.get(key, "")).strip()
        if key == "Next Steps":
            project_details[key] = value or "Not set"
        elif value:
            project_details[key] = value

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
    show_sra = sra_toggle if sra_toggle is not None else bool(sra_steps)
    show_nva = nva_toggle if nva_toggle is not None else bool(nva_steps)

    return {
        "project_details": project_details,
        "show_sra": show_sra,
        "show_nva": show_nva,
        "sra_steps": build_step_display(sra_steps, sra_field_keys, can_edit=can_edit),
        "nva_steps": build_step_display(nva_steps, nva_field_keys, can_edit=can_edit),
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

    return render_template(
        "admin_projects.html",
        completed_groups=group_projects_for_admin(completed),
        active_groups=group_projects_for_admin(active),
        count=len(projects),
        completed_count=len(completed),
        active_count=len(active),
        completed_status_counts=status_counts(completed),
        active_status_counts=status_counts(active),
    )


@app.post("/status/<sf_id>/update-date")
def update_date(sf_id: str):
    signature = request.args.get("sig", "")
    if not valid_client_signature(sf_id, signature):
        abort(403)
    if not has_admin_edit_access():
        abort(403)

    metric_key = str(request.form.get("metric_key", "")).strip()
    override_key = str(request.form.get("override_key", "")).strip()
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
    dashboard = build_dashboard_view(status, ecd_overrides=overrides, can_edit=can_edit)
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
