from __future__ import annotations

import copy
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
REQUEST_TIMEOUT = (5, 30)
SENSITIVE_PATTERNS = [
    re.compile(r"(Authorization\s*[:=]\s*)([^,\s}]+)", re.IGNORECASE),
    re.compile(r"(ApiKey\s+)([A-Za-z0-9+/=._-]+)", re.IGNORECASE),
    re.compile(r"(Basic\s+)([A-Za-z0-9+/=._-]+)", re.IGNORECASE),
    re.compile(r"(password\s*[:=]\s*)([^,\s}]+)", re.IGNORECASE),
    re.compile(r"(api[_-]?key\s*[:=]\s*)([^,\s}]+)", re.IGNORECASE),
    re.compile(r"(token\s*[:=]\s*)([^,\s}]+)", re.IGNORECASE),
]

__all__ = [
    "DEFAULT_FIELD_LIMIT",
    "FIELD_LIMIT_KEY",
    "ReadOnlyRequestError",
    "UnsafeWriteRequestError",
    "build_field_limit_excel",
    "build_instance_summary",
    "build_update_preview",
    "encoded_path",
    "extract_data_views",
    "extract_indices_from_cat",
    "extract_indices_from_resolve",
    "extract_templates",
    "mask_sensitive",
    "match_template",
    "match_templates",
    "merge_template_limit",
    "now_ts",
    "parse_total_fields_limit",
    "readonly_get",
    "safe_put_index_field_limit",
    "safe_put_template",
]


class ReadOnlyRequestError(ValueError):
    pass


class UnsafeWriteRequestError(ValueError):
    pass


def mask_sensitive(value: object) -> str:
    text = str(value or "")
    for pattern in SENSITIVE_PATTERNS:
        text = pattern.sub(r"\1***", text)
    return text[:1000]


def _response(method: str, url: str, endpoint: str, auth_headers: Dict[str, str], params: Dict[str, Any] | None = None, json_body: Dict[str, Any] | None = None, timeout: int = REQUEST_TIMEOUT) -> Dict[str, Any]:
    try:
        response = requests.request(method=method, url=url, headers=dict(auth_headers or {}), params=params, json=json_body, timeout=timeout)
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


def readonly_get(base_url: str, endpoint: str, auth_headers: Dict[str, str], params: Dict[str, Any] | None = None, extra_headers: Dict[str, str] | None = None, timeout: int = REQUEST_TIMEOUT) -> Dict[str, Any]:
    """Perform a guarded read-only GET request and mask sensitive error details."""
    if not endpoint.startswith("/"):
        endpoint = f"/{endpoint}"
    if extra_headers:
        auth_headers = {**(auth_headers or {}), **extra_headers}
    return _response("GET", f"{base_url.rstrip('/')}{endpoint}", endpoint, auth_headers, params=params, timeout=timeout)


def safe_put_index_field_limit(base_url: str, index_name: str, new_limit: int, auth_headers: Dict[str, str]) -> Dict[str, Any]:
    if int(new_limit) <= DEFAULT_FIELD_LIMIT:
        raise UnsafeWriteRequestError("New limit must be greater than 1000")
    endpoint = f"/{encoded_path(index_name)}/_settings"
    body = {FIELD_LIMIT_KEY: int(new_limit)}
    return _response("PUT", f"{base_url.rstrip('/')}{endpoint}", endpoint, auth_headers, json_body=body)


def safe_put_template(base_url: str, template_name: str, template_type: str, body: Dict[str, Any], auth_headers: Dict[str, str]) -> Dict[str, Any]:
    if template_type not in {"composable", "legacy"} or not template_name or not isinstance(body, dict):
        raise UnsafeWriteRequestError("Unsafe template update request")
    endpoint = f"/_index_template/{encoded_path(template_name)}" if template_type == "composable" else f"/_template/{encoded_path(template_name)}"
    return _response("PUT", f"{base_url.rstrip('/')}{endpoint}", endpoint, auth_headers, json_body=body)


def extract_data_views(payload: Any, legacy: bool = False) -> List[Dict[str, str]]:
    """Compatibility helper retained for older tests; not used by the primary audit."""
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

    configured_value = None
    raw_setting_path = ""
    if isinstance(settings, dict):
        if FIELD_LIMIT_KEY in settings:
            configured_value = settings.get(FIELD_LIMIT_KEY)
            raw_setting_path = FIELD_LIMIT_KEY
        else:
            configured_value = (((settings.get("index") or {}).get("mapping") or {}).get("total_fields") or {}).get("limit")
            if configured_value is not None:
                raw_setting_path = "settings.index.mapping.total_fields.limit"

    if configured_value is None:
        return {
            "configured_limit": None,
            "configured_limit_status": "not_configured",
            "effective_default": DEFAULT_FIELD_LIMIT,
            "above_1000": False,
            "raw_setting_path": "",
            "status": "not_configured",
            "error_message": "",
        }

    try:
        numeric = int(configured_value)
    except (TypeError, ValueError):
        return {
            "configured_limit": configured_value,
            "configured_limit_status": "parse_error",
            "effective_default": None,
            "above_1000": False,
            "raw_setting_path": raw_setting_path,
            "status": "error",
            "error_message": f"Non-numeric {FIELD_LIMIT_KEY}: {configured_value}",
        }

    return {
        "configured_limit": configured_value,
        "configured_limit_status": "configured",
        "effective_default": None,
        "above_1000": numeric > DEFAULT_FIELD_LIMIT,
        "raw_setting_path": raw_setting_path,
        "status": "configured_above_1000" if numeric > DEFAULT_FIELD_LIMIT else "configured_1000_or_less",
        "error_message": "",
    }


def extract_indices_from_cat(payload: Any) -> List[str]:
    if not isinstance(payload, list):
        return []
    return list(dict.fromkeys(str(item.get("index")) for item in payload if isinstance(item, dict) and item.get("index")))


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
        iterable = [(name, body, "legacy") for name, body in payload.items() if isinstance(body, dict)]
    else:
        iterable = [(item.get("name"), item.get("index_template", {}), "composable") for item in payload.get("index_templates", []) or [] if isinstance(item, dict)]
    for name, body, template_type in iterable:
        if not isinstance(body, dict):
            continue
        patterns = body.get("index_patterns") or []
        settings = ((body.get("template") or {}).get("settings") or body.get("settings") or {})
        limit = _extract_template_limit(settings)
        templates.append({"name": str(name or ""), "index_patterns": patterns, "limit": limit or "", "template_type": template_type})
    return templates


def match_template(templates: List[Dict[str, Any]], index_pattern: str, index_name: str) -> Tuple[str, Any]:
    matched = [t for t in match_templates(templates, index_name)]
    if len(matched) == 1:
        return str(matched[0].get("name") or ""), matched[0].get("limit") or ""
    return "", ""


def match_templates(templates: List[Dict[str, Any]], index_name: str) -> List[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []
    for template in templates:
        for pattern in template.get("index_patterns") or []:
            if pattern == index_name or fnmatch(index_name, pattern) or (str(pattern).endswith("*") and index_name.startswith(str(pattern)[:-1])):
                matches.append(template)
                break
    return matches


def build_instance_summary(instance: Dict[str, str], detail_rows: List[Dict[str, Any]], data_views_checked: int = 0, fatal_error: str = "") -> Dict[str, Any]:
    successful = [r for r in detail_rows if r.get("status") != "error"]
    failed = [r for r in detail_rows if r.get("status") == "error"]
    configured = [r for r in successful if r.get("configured_limit_status") == "configured"]
    not_configured = [r for r in successful if r.get("configured_limit_status") == "not_configured"]
    above = [r for r in configured if bool(r.get("above_1000"))]
    max_limits = [int(r["configured_limit"]) for r in configured if str(r.get("configured_limit", "")).isdigit()]
    status = "failed" if fatal_error else ("partial" if successful and failed else ("failed" if failed and not successful else "ok"))
    return {
        "instance": instance.get("name", ""), "base_url": instance.get("base_url", ""), "auth_status": "authenticated",
        "indices_found": len(detail_rows), "indices_checked": len(successful), "indices_with_configured_limit": len(configured),
        "indices_above_1000": len(above), "indices_not_configured": len(not_configured), "indices_failed": len(failed),
        "max_detected_limit": max(max_limits) if max_limits else "",
        "workaround_detected": bool(above), "status": status, "error_message": fatal_error,
    }


def build_update_preview(field_limit_rows: List[Dict[str, Any]], instance_names: List[str], selected_keys: set[tuple[str, str]], apply_mode: str, new_limit: int, update_templates: bool, dry_run: bool) -> List[Dict[str, Any]]:
    preview: List[Dict[str, Any]] = []
    for row in field_limit_rows:
        if row.get("instance") not in instance_names:
            continue
        key = (str(row.get("instance", "")), str(row.get("index_name", "")))
        if apply_mode == "selected" and key not in selected_keys:
            continue

        status = str(row.get("status", ""))
        configured_status = str(row.get("configured_limit_status", ""))
        configured_limit = row.get("configured_limit")
        effective_default = row.get("effective_default")
        reason = ""
        update_required = False

        if status == "error" or configured_status in {"parse_error", "request_error"}:
            reason = configured_status or "request_error"
        elif configured_status == "not_configured":
            reason = "not_configured_effective_default_1000"
            update_required = int(effective_default or DEFAULT_FIELD_LIMIT) < int(new_limit)
        elif str(configured_limit).isdigit() and int(configured_limit) < int(new_limit):
            reason = "configured_below_new_limit"
            update_required = True
        else:
            reason = "already_at_or_above_new_limit"

        if apply_mode == "default" and configured_status != "not_configured":
            continue
        if apply_mode == "lower" and not update_required:
            continue

        preview.append({
            "instance": row.get("instance", ""),
            "base_url": row.get("base_url", ""),
            "index_name": row.get("index_name", ""),
            "configured_limit": configured_limit,
            "effective_default": effective_default,
            "new_limit": int(new_limit),
            "update_required": bool(update_required),
            "reason": reason,
            "template_name": row.get("template_name", ""),
            "template_current_limit": row.get("template_limit", ""),
            "template_new_limit": int(new_limit) if update_templates and row.get("template_name") else "",
            "action": "dry_run" if dry_run else "update",
            "status": "ready" if update_required else "skipped",
        })
    return preview


def merge_template_limit(template_body: Dict[str, Any], template_type: str, new_limit: int) -> Dict[str, Any]:
    body = copy.deepcopy(template_body)
    if template_type == "composable":
        if "index_templates" in body and isinstance(body.get("index_templates"), list) and len(body["index_templates"]) == 1:
            body = copy.deepcopy(body["index_templates"][0])
        if "index_template" in body and isinstance(body["index_template"], dict):
            body = copy.deepcopy(body["index_template"])
        if not isinstance(body, dict):
            raise UnsafeWriteRequestError("Invalid composable template body")
        body.setdefault("template", {}).setdefault("settings", {})[FIELD_LIMIT_KEY] = int(new_limit)
        return body
    if template_type == "legacy":
        if len(body) == 1 and all(isinstance(v, dict) for v in body.values()):
            body = copy.deepcopy(next(iter(body.values())))
        if not isinstance(body, dict):
            raise UnsafeWriteRequestError("Invalid legacy template body")
        body.setdefault("settings", {})[FIELD_LIMIT_KEY] = int(new_limit)
        return body
    raise UnsafeWriteRequestError("Unknown template type")


def build_field_limit_excel(instance_rows: List[Dict[str, Any]], technical_rows: List[Dict[str, Any]], error_rows: List[Dict[str, Any]], log_rows: List[Dict[str, Any]] | None = None, update_preview: List[Dict[str, Any]] | None = None, update_results: List[Dict[str, Any]] | None = None, template_update_results: List[Dict[str, Any]] | None = None, template_details: List[Dict[str, Any]] | None = None) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame(instance_rows).to_excel(writer, index=False, sheet_name="instance_limits")
        if update_preview:
            pd.DataFrame(update_preview).to_excel(writer, index=False, sheet_name="update_preview")
        combined_update_results = (update_results or []) + (template_update_results or [])
        if combined_update_results:
            pd.DataFrame(combined_update_results).to_excel(writer, index=False, sheet_name="update_results")
        technical_details = (technical_rows or []) + (template_details or [])
        if technical_details:
            pd.DataFrame(technical_details).to_excel(writer, index=False, sheet_name="technical_details")
        combined_logs = (error_rows or []) + (log_rows or [])
        if combined_logs:
            pd.DataFrame(combined_logs).to_excel(writer, index=False, sheet_name="errors_logs")
    return buffer.getvalue()


def now_ts() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def encoded_path(value: str) -> str:
    return quote(value, safe="*,-_.")
