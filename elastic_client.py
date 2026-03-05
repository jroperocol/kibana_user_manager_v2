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
) -> Dict[str, Any]:
    try:
        response = requests.request(
            method=method,
            url=url,
            headers=auth_headers,
            json=json_body,
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


def test_connection(base_url: str, auth_headers: Dict[str, str]) -> Dict[str, Any]:
    """Uses list_users endpoint as connectivity test."""
    return list_users(base_url, auth_headers)
