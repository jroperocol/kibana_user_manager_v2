from __future__ import annotations

import csv
import io
import json
import re
from typing import Dict, List, Tuple
from urllib.parse import urlparse


_HEADER_ALIASES = {
    "name": "name",
    "nombre": "name",
    "instance": "name",
    "instancia": "name",
    "cluster": "name",
    "tenant": "name",
    "baseurl": "base_url",
    "url": "base_url",
    "endpoint": "base_url",
    "kibanaurl": "base_url",
    "elasticurl": "base_url",
    "link": "base_url",
}


def _decode_bytes(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def _normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").strip().lower())


def _guess_delimiter(text: str) -> str:
    sample = "\n".join(text.splitlines()[:10])
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return dialect.delimiter
    except csv.Error:
        return ","


def _clean_cell(value: str) -> str:
    return (value or "").strip().strip('"').strip("'").strip()


def _fix_url(value: str) -> str:
    cleaned = _clean_cell(value)
    cleaned = re.sub(r"\s+", "", cleaned)
    if cleaned.startswith("https:/") and not cleaned.startswith("https://"):
        cleaned = cleaned.replace("https:/", "https://", 1)
    if cleaned.startswith("http:/") and not cleaned.startswith("http://"):
        cleaned = cleaned.replace("http:/", "http://", 1)
    return cleaned.rstrip("/")


def _extract_pair_from_single_cell(value: str) -> Tuple[str, str]:
    raw = _clean_cell(value)
    if not raw:
        return "", ""
    for delimiter in (",", ";", "\t", "|"):
        parts = [part.strip() for part in raw.split(delimiter)]
        if len(parts) >= 2:
            return parts[0], parts[1]
    return raw, ""


def validate_base_url(base_url: str) -> Tuple[bool, str]:
    base_url = (base_url or "").strip()
    if not base_url:
        return False, "base_url is required"
    if not (base_url.startswith("https://") or base_url.startswith("http://")):
        return False, "base_url must start with http:// or https://"

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

    text = _decode_bytes(content)
    if not text.strip():
        return [], ["CSV file is empty"]

    delimiter = _guess_delimiter(text)
    rows = list(csv.reader(io.StringIO(text), delimiter=delimiter))
    if not rows:
        return [], ["CSV file is empty"]

    first_row = rows[0]
    canonical_header_positions: Dict[str, int] = {}
    for index, header in enumerate(first_row):
        canonical = _HEADER_ALIASES.get(_normalize_header(header), "")
        if canonical and canonical not in canonical_header_positions:
            canonical_header_positions[canonical] = index

    has_valid_header = {"name", "base_url"}.issubset(canonical_header_positions.keys())
    data_start_index = 1 if has_valid_header else 0

    if not has_valid_header:
        errors.append(
            "CSV headers no detectados; se asumieron las primeras 2 columnas como name/base_url."
        )

    for row_index, row in enumerate(rows[data_start_index:], start=data_start_index + 1):
        if not row or not any(_clean_cell(cell) for cell in row):
            continue

        if has_valid_header:
            raw_name = row[canonical_header_positions["name"]] if canonical_header_positions["name"] < len(row) else ""
            raw_base_url = row[canonical_header_positions["base_url"]] if canonical_header_positions["base_url"] < len(row) else ""
        else:
            raw_name = row[0] if len(row) >= 1 else ""
            raw_base_url = row[1] if len(row) >= 2 else ""

        if len(row) == 1:
            inferred_name, inferred_base_url = _extract_pair_from_single_cell(row[0])
            if inferred_base_url:
                raw_name = inferred_name
                raw_base_url = inferred_base_url
            else:
                raw_name = raw_name or inferred_name
                raw_base_url = raw_base_url or inferred_base_url

        normalized = {
            "name": _clean_cell(raw_name),
            "base_url": _fix_url(raw_base_url),
        }

        if not normalized["name"] and not normalized["base_url"]:
            continue

        valid, err = validate_instance_row(normalized)
        if not valid:
            errors.append(f"Line {row_index}: {err}")
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
