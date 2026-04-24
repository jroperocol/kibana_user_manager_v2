from __future__ import annotations

from typing import Any, Dict, List, Optional

import requests

DEFAULT_TIMEOUT = 10


class ElasticClientError(Exception):
    pass


def _request(
    method: str,
    url: str,
    auth_headers: Dict[str, str],
    json_body: Dict[str, Any] | None = None,
    timeout: int = DEFAULT_TIMEOUT,
    params: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    try:
        response = requests.request(
            method=method,
            url=url,
            headers=auth_headers,
            json=json_body,
            params=params,
            timeout=timeout,
        )
    except requests.exceptions.SSLError as exc:
        return {"ok": False, "status_code": None, "message": f"SSL error: {exc}"}
    except requests.exceptions.Timeout:
        return {"ok": False, "status_code": None, "message": "Request timed out."}
    except requests.exceptions.RequestException as exc:
        return {"ok": False, "status_code": None, "message": f"Connection error: {exc}"}

    if response.status_code >= 400:
        detail = response.text.strip()
        if len(detail) > 500:
            detail = detail[:500] + "..."
        return {
            "ok": False,
            "status_code": response.status_code,
            "message": detail or response.reason,
        }

    if not response.text:
        return {"ok": True, "status_code": response.status_code, "data": {}}

    try:
        return {"ok": True, "status_code": response.status_code, "data": response.json()}
    except ValueError:
        return {
            "ok": True,
            "status_code": response.status_code,
            "data": {"raw": response.text},
        }


def list_users(base_url: str, auth_headers: Dict[str, str]) -> Dict[str, Any]:
    """GET /_security/user"""
    return _request("GET", f"{base_url}/_security/user", auth_headers)


def create_user(
    base_url: str,
    auth_headers: Dict[str, str],
    username: str,
    password: str,
    roles: List[str],
    full_name: Optional[str] = None,
    email: Optional[str] = None,
) -> Dict[str, Any]:
    """PUT /_security/user/{username}"""
    payload = {"password": password, "roles": roles}
    if full_name:
        payload["full_name"] = full_name
    if email:
        payload["email"] = email
    return _request("PUT", f"{base_url}/_security/user/{username}", auth_headers, json_body=payload)


def delete_user(base_url: str, auth_headers: Dict[str, str], username: str) -> Dict[str, Any]:
    """DELETE /_security/user/{username}"""
    return _request("DELETE", f"{base_url}/_security/user/{username}", auth_headers)


def list_roles(base_url: str, auth_headers: Dict[str, str]) -> Dict[str, Any]:
    """GET /_security/role"""
    return _request("GET", f"{base_url}/_security/role", auth_headers)


def list_indices(base_url: str, auth_headers: Dict[str, str], pattern: str = "*") -> Dict[str, Any]:
    """GET /_cat/indices/{pattern}?format=json"""
    safe_pattern = (pattern or "*").strip() or "*"
    response = _request("GET", f"{base_url}/_cat/indices/{safe_pattern}", auth_headers, params={"format": "json"})
    if not response.get("ok"):
        return {
            "ok": False,
            "status_code": response.get("status_code"),
            "message": f"Failed to list indices: {response.get('message', 'Unknown error')}",
        }

    payload = response.get("data", [])
    if not isinstance(payload, list):
        return {
            "ok": False,
            "status_code": response.get("status_code"),
            "message": "Failed to list indices: invalid response payload",
        }

    normalized = [
        {
            "index": item.get("index", ""),
            "health": item.get("health", ""),
            "status": item.get("status", ""),
            "docs.count": item.get("docs.count", ""),
            "store.size": item.get("store.size", ""),
        }
        for item in payload
        if isinstance(item, dict)
    ]
    return {"ok": True, "status_code": response.get("status_code"), "data": normalized}


def search_index(base_url: str, auth_headers: Dict[str, str], index_name: str, body: Dict[str, Any]) -> Dict[str, Any]:
    """POST /{index}/_search"""
    response = _request("POST", f"{base_url}/{index_name}/_search", auth_headers, json_body=body)
    if not response.get("ok"):
        return {
            "ok": False,
            "status_code": response.get("status_code"),
            "message": f"Failed to search index '{index_name}': {response.get('message', 'Unknown error')}",
        }
    return response


def test_connection(base_url: str, auth_headers: Dict[str, str]) -> Dict[str, Any]:
    """Uses list_users endpoint as connectivity test."""
    return list_users(base_url, auth_headers)
