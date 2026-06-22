from __future__ import annotations

import io
import re
from datetime import datetime
from fnmatch import fnmatch
from typing import Any, Dict, List, Tuple
from urllib.parse import quote

import pandas as pd
import requests

DEFAULT_FIELD_LIMIT = 1000
FIELD_LIMIT_KEY = "index.mapping.total_fields.limit"
REQUEST_TIMEOUT = 20
SENSITIVE_PATTERNS = [
    re.compile(r"(Authorization\s*[:=]\s*)([^,\s}]+)", re.IGNORECASE),
    re.compile(r"(ApiKey\s+)([A-Za-z0-9+/=._-]+)", re.IGNORECASE),
    re.compile(r"(Basic\s+)([A-Za-z0-9+/=._-]+)", re.IGNORECASE),
    re.compile(r"(password\s*[:=]\s*)([^,\s}]+)", re.IGNORECASE),
    re.compile(r"(api[_-]?key\s*[:=]\s*)([^,\s}]+)", re.IGNORECASE),
]


class ReadOnlyRequestError(ValueError):
    pass


def mask_sensitive(value: object) -> str:
    text = str(value or "")
    for pattern in SENSITIVE_PATTERNS:
        text = pattern.sub(r"\1***", text)
    return text[:1000]


def readonly_get(base_url: str, endpoint: str, auth_headers: Dict[str, str], params: Dict[str, Any] | None = None, extra_headers: Dict[str, str] | None = None, timeout: int = REQUEST_TIMEOUT) -> Dict[str, Any]:
    """Perform a guarded read-only GET request and mask sensitive error details."""
    if not endpoint.startswith("/"):
        endpoint = f"/{endpoint}"
    method = "GET"
    if method != "GET":
        raise ReadOnlyRequestError("Field limit audit only allows GET requests")
    safe_headers = dict(auth_headers or {})
    if extra_headers:
        safe_headers.update(extra_headers)
    url = f"{base_url.rstrip('/')}{endpoint}"
    try:
        response = requests.request(method=method, url=url, headers=safe_headers, params=params, timeout=timeout)
    except requests.exceptions.SSLError as exc:
        return {"ok": False, "status_code": None, "message": mask_sensitive(f"SSL error: {exc}"), "endpoint": endpoint}
    except requests.exceptions.Timeout:
        return {"ok": False, "status_code": None, "message": "Request timed out.", "endpoint": endpoint}
    except requests.exceptions.RequestException as exc:
        return {"ok": False, "status_code": None, "message": mask_sensitive(f"Connection error: {exc}"), "endpoint": endpoint}

    if response.status_code >= 400:
        return {"ok": False, "status_code": response.status_code, "message": mask_sensitive(response.text.strip() or response.reason), "endpoint": endpoint}
    if not response.text:
        return {"ok": True, "status_code": response.status_code, "data": {}, "endpoint": endpoint}
    try:
        data = response.json()
    except ValueError:
        data = {"raw": response.text}
    return {"ok": True, "status_code": response.status_code, "data": data, "endpoint": endpoint}


def extract_data_views(payload: Any, legacy: bool = False) -> List[Dict[str, str]]:
    views: List[Dict[str, str]] = []
    if not isinstance(payload, dict):
        return views
    if legacy:
        for obj in payload.get("saved_objects", []) or []:
            if not isinstance(obj, dict):
                continue
            title = ((obj.get("attributes") or {}).get("title") or "").strip()
            if title:
                views.append({"id": str(obj.get("id") or title), "title": title, "name": str((obj.get("attributes") or {}).get("name") or title)})
        return views
    candidates = payload.get("data_view") or payload.get("data_views") or payload.get("index_patterns") or []
    if isinstance(candidates, dict):
        candidates = [candidates]
    for item in candidates or []:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or item.get("name") or item.get("id") or "").strip()
        if title:
            views.append({"id": str(item.get("id") or title), "title": title, "name": str(item.get("name") or title)})
    return views


def parse_total_fields_limit(settings_payload: Any, index_name: str | None = None) -> Dict[str, Any]:
    settings: Dict[str, Any] = {}
    if isinstance(settings_payload, dict):
        if index_name and isinstance(settings_payload.get(index_name), dict):
            settings = settings_payload[index_name].get("settings", {}) or {}
        elif len(settings_payload) == 1 and all(isinstance(v, dict) and "settings" in v for v in settings_payload.values()):
            settings = next(iter(settings_payload.values())).get("settings", {}) or {}
        else:
            settings = settings_payload.get("settings", settings_payload) or {}
    value = settings.get(FIELD_LIMIT_KEY)
    if value is None:
        value = (((settings.get("index") or {}).get("mapping") or {}).get("total_fields") or {}).get("limit")
    if value is None:
        return {"total_fields_limit": DEFAULT_FIELD_LIMIT, "default_assumed": True, "above_1000": False, "status": "default_or_missing", "error_message": ""}
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return {"total_fields_limit": "", "default_assumed": False, "above_1000": False, "status": "error", "error_message": f"Non-numeric {FIELD_LIMIT_KEY}: {value}"}
    if numeric > DEFAULT_FIELD_LIMIT:
        status = "workaround_detected"
    elif numeric == DEFAULT_FIELD_LIMIT:
        status = "default_or_missing"
    else:
        status = "below_default"
    return {"total_fields_limit": numeric, "default_assumed": False, "above_1000": numeric > DEFAULT_FIELD_LIMIT, "status": status, "error_message": ""}


def extract_indices_from_resolve(payload: Any) -> List[str]:
    if not isinstance(payload, dict):
        return []
    indices = []
    for key in ("indices", "aliases", "data_streams"):
        for item in payload.get(key, []) or []:
            name = item.get("name") if isinstance(item, dict) else None
            if name:
                indices.append(str(name))
    return list(dict.fromkeys(indices))


def _extract_template_limit(settings: Dict[str, Any]) -> Any:
    return settings.get(FIELD_LIMIT_KEY) or ((((settings.get("index") or {}).get("mapping") or {}).get("total_fields") or {}).get("limit"))


def extract_templates(payload: Any, legacy: bool = False) -> List[Dict[str, Any]]:
    templates: List[Dict[str, Any]] = []
    if not isinstance(payload, dict):
        return templates
    if legacy:
        items = [{"name": name, **body} for name, body in payload.items() if isinstance(body, dict)]
    else:
        items = payload.get("index_templates", []) or []
    for item in items:
        name = str(item.get("name") or "")
        body = item.get("index_template", item) or {}
        patterns = body.get("index_patterns") or []
        settings = ((body.get("template") or {}).get("settings") or body.get("settings") or {})
        limit = _extract_template_limit(settings)
        templates.append({"name": name, "index_patterns": patterns, "limit": limit})
    return templates


def match_template(templates: List[Dict[str, Any]], index_pattern: str, index_name: str) -> Tuple[str, Any]:
    for template in templates:
        for pattern in template.get("index_patterns") or []:
            if fnmatch(index_name, pattern) or fnmatch(index_pattern, pattern) or fnmatch(pattern, index_pattern):
                return str(template.get("name") or ""), template.get("limit") or ""
    return "", ""


def build_instance_summary(instance: Dict[str, str], detail_rows: List[Dict[str, Any]], data_views_checked: int, fatal_error: str = "") -> Dict[str, Any]:
    successful = [r for r in detail_rows if r.get("status") != "error"]
    failed = [r for r in detail_rows if r.get("status") == "error"]
    above = [r for r in successful if bool(r.get("above_1000"))]
    default = [r for r in successful if r.get("status") == "default_or_missing"]
    max_limits = [int(r["total_fields_limit"]) for r in successful if str(r.get("total_fields_limit", "")).isdigit()]
    if fatal_error:
        status = "failed"
    elif detail_rows and successful and failed:
        status = "partial"
    elif detail_rows and not successful:
        status = "failed"
    elif above:
        status = "workaround_detected"
    else:
        status = "default_only"
    return {
        "instance": instance.get("name", ""), "base_url": instance.get("base_url", ""), "auth_status": "authenticated",
        "data_views_checked": data_views_checked, "indices_checked": len({r.get("matched_index") for r in successful if r.get("matched_index")}),
        "indices_above_1000": len({r.get("matched_index") for r in above if r.get("matched_index")}),
        "indices_default_or_missing": len({r.get("matched_index") for r in default if r.get("matched_index")}),
        "indices_failed": len(failed), "max_detected_limit": max(max_limits) if max_limits else "",
        "workaround_detected": bool(above), "status": status, "error_message": fatal_error,
    }


def build_field_limit_excel(summary_rows: List[Dict[str, Any]], detail_rows: List[Dict[str, Any]], error_rows: List[Dict[str, Any]], log_rows: List[Dict[str, Any]] | None = None) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame(summary_rows).to_excel(writer, index=False, sheet_name="Summary")
        pd.DataFrame(detail_rows).to_excel(writer, index=False, sheet_name="Details")
        pd.DataFrame(error_rows).to_excel(writer, index=False, sheet_name="Errors")
        if log_rows:
            pd.DataFrame(log_rows).to_excel(writer, index=False, sheet_name="Logs")
    return buffer.getvalue()


def now_ts() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def encoded_path(value: str) -> str:
    return quote(value, safe="*,-_.")
