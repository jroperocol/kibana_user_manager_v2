from __future__ import annotations

import csv
import io
import json
from typing import Dict, List, Tuple
from urllib.parse import urlparse


def validate_base_url(base_url: str) -> Tuple[bool, str]:
    base_url = (base_url or "").strip()
    if not base_url:
        return False, "base_url is required"
    if not base_url.startswith("https://"):
        return False, "base_url must start with https://"

    parsed = urlparse(base_url)
    if not parsed.netloc:
        return False, "base_url is invalid"

    return True, ""


def validate_instance_row(row: Dict[str, str]) -> Tuple[bool, str]:
    name = (row.get("name") or "").strip()
    base_url = (row.get("base_url") or "").strip()

    if not name:
        return False, "name is required"

    valid_url, error = validate_base_url(base_url)
    if not valid_url:
        return False, error

    return True, ""


def load_instances_from_csv(content: bytes) -> Tuple[List[Dict[str, str]], List[str]]:
    instances: List[Dict[str, str]] = []
    errors: List[str] = []

    text_buffer = io.StringIO(content.decode("utf-8"))
    reader = csv.DictReader(text_buffer)

    expected = {"name", "base_url"}
    if not reader.fieldnames or not expected.issubset(set(reader.fieldnames)):
        return [], ["CSV must include columns: name, base_url"]

    for idx, row in enumerate(reader, start=2):
        normalized = {
            "name": (row.get("name") or "").strip(),
            "base_url": (row.get("base_url") or "").strip().rstrip("/"),
        }
        valid, err = validate_instance_row(normalized)
        if not valid:
            errors.append(f"Line {idx}: {err}")
            continue
        instances.append(normalized)

    return instances, errors


def load_instances_from_json(content: bytes) -> Tuple[List[Dict[str, str]], List[str]]:
    try:
        payload = json.loads(content.decode("utf-8"))
    except json.JSONDecodeError as exc:
        return [], [f"Invalid JSON: {exc}"]

    if not isinstance(payload, list):
        return [], ["JSON must be an array of objects with name and base_url"]

    instances: List[Dict[str, str]] = []
    errors: List[str] = []

    for idx, item in enumerate(payload, start=1):
        if not isinstance(item, dict):
            errors.append(f"Item {idx}: each item must be an object")
            continue

        normalized = {
            "name": (item.get("name") or "").strip(),
            "base_url": (item.get("base_url") or "").strip().rstrip("/"),
        }
        valid, err = validate_instance_row(normalized)
        if not valid:
            errors.append(f"Item {idx}: {err}")
            continue

        instances.append(normalized)

    return instances, errors
