from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple


MAX_UUID_RECORDS = 10000
UUID_PAGE_SIZE = 1000
SYSTEM_INDEX_PREFIXES = (
    ".",
    ".apm-",
    ".async-search",
    ".ds-",
    ".kibana",
    ".security",
    ".internal-",
    ".monitoring",
    ".logs-",
    ".metrics-",
    ".slm-history",
    ".inference",
)


def to_es_datetime(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat() + "Z"


def build_date_range(
    period: str,
    custom_start: Optional[date] = None,
    custom_end: Optional[date] = None,
    now: Optional[datetime] = None,
) -> Tuple[str, datetime, datetime]:
    now_dt = now or datetime.utcnow()

    if period == "Today":
        start = datetime.combine(now_dt.date(), time.min)
        return period, start, now_dt

    if period == "Last 24 hours":
        return period, now_dt - timedelta(hours=24), now_dt

    day_mapping = {
        "Last 7 days": 7,
        "Last 30 days": 30,
        "Last 60 days": 60,
        "Last 90 days": 90,
    }
    if period in day_mapping:
        return period, now_dt - timedelta(days=day_mapping[period]), now_dt

    if period == "Custom date range":
        if custom_start is None or custom_end is None:
            raise ValueError("Custom date range requires start and end dates")
        start = datetime.combine(custom_start, time.min)
        end = datetime.combine(custom_end, time.max)
        if start > end:
            raise ValueError("Start date cannot be after end date")
        return period, start, end

    raise ValueError(f"Unsupported period: {period}")


def list_indices(
    instance: Dict[str, str],
    auth_headers: Dict[str, str],
    fetch_indices_fn: Callable[..., Dict[str, Any]],
    pattern: str = "*_ivrs-*",
) -> Dict[str, Any]:
    return fetch_indices_fn(instance["base_url"], auth_headers, pattern=pattern)


def is_system_index(name: str) -> bool:
    return str(name or "").startswith(SYSTEM_INDEX_PREFIXES)


def filter_indices(indices: List[Dict[str, Any]], include_system: bool = False) -> List[Dict[str, Any]]:
    if include_system:
        return indices
    return [item for item in indices if not is_system_index(item.get("index", ""))]


def count_index_activity(
    instance: Dict[str, str],
    index_name: str,
    start: datetime,
    end: datetime,
    timestamp_field: str,
    auth_headers: Dict[str, str],
    search_fn: Callable[..., Dict[str, Any]],
) -> Dict[str, Any]:
    body = {
        "size": 0,
        "track_total_hits": True,
        "query": {
            "range": {
                timestamp_field: {
                    "gte": to_es_datetime(start),
                    "lte": to_es_datetime(end),
                }
            }
        },
    }
    response = search_fn(instance["base_url"], auth_headers, index_name, body)
    if not response.get("ok"):
        return response

    total_hits = response.get("data", {}).get("hits", {}).get("total", 0)
    if isinstance(total_hits, dict):
        total_value = int(total_hits.get("value", 0))
    else:
        total_value = int(total_hits or 0)

    return {
        "ok": True,
        "status_code": response.get("status_code"),
        "count": total_value,
    }


def build_activity_agg_query(start: datetime, end: datetime, timestamp_field: str) -> Dict[str, Any]:
    return {
        "size": 0,
        "track_total_hits": True,
        "query": {
            "range": {
                timestamp_field: {
                    "gte": to_es_datetime(start),
                    "lt": to_es_datetime(end),
                }
            }
        },
        "aggs": {
            "by_index": {
                "terms": {
                    "field": "_index",
                    "size": 10000,
                }
            }
        },
    }


def parse_activity_buckets(response: Dict[str, Any], include_system: bool = False) -> Dict[str, int]:
    buckets = response.get("data", {}).get("aggregations", {}).get("by_index", {}).get("buckets", [])
    counts: Dict[str, int] = {}
    for bucket in buckets:
        index_name = str(bucket.get("key", ""))
        if not index_name:
            continue
        if not include_system and is_system_index(index_name):
            continue
        counts[index_name] = int(bucket.get("doc_count", 0))
    return counts


def build_error_row(instance: Dict[str, str], period_label: str, start: datetime, end: datetime, error: str) -> Dict[str, Any]:
    return {
        "report_date": datetime.utcnow().date().isoformat(),
        "instance_name": instance.get("name", ""),
        "base_url": instance.get("base_url", ""),
        "index_name": "",
        "period_label": period_label,
        "start_datetime": to_es_datetime(start),
        "end_datetime": to_es_datetime(end),
        "activity_count": 0,
        "has_activity": False,
        "error": error,
    }


def build_activity_rows_for_instance(
    instance: Dict[str, str],
    period_label: str,
    start: datetime,
    end: datetime,
    counts_by_index: Dict[str, int],
    loaded_indices: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    base = {
        "report_date": datetime.utcnow().date().isoformat(),
        "instance_name": instance.get("name", ""),
        "base_url": instance.get("base_url", ""),
        "period_label": period_label,
        "start_datetime": to_es_datetime(start),
        "end_datetime": to_es_datetime(end),
    }

    rows: List[Dict[str, Any]] = []
    loaded_set = set(loaded_indices or [])
    target_indices = sorted(set(counts_by_index.keys()) | loaded_set) if loaded_set else sorted(counts_by_index.keys())
    for index_name in target_indices:
        count_value = int(counts_by_index.get(index_name, 0))
        rows.append(
            {
                **base,
                "index_name": index_name,
                "activity_count": count_value,
                "has_activity": count_value > 0,
            }
        )
    return rows


def fetch_index_uuids(
    instance: Dict[str, str],
    index_name: str,
    start: datetime,
    end: datetime,
    timestamp_field: str,
    uuid_field: str,
    auth_headers: Dict[str, str],
    search_fn: Callable[..., Dict[str, Any]],
    max_records: int = MAX_UUID_RECORDS,
    page_size: int = UUID_PAGE_SIZE,
) -> Dict[str, Any]:
    uuids: List[str] = []
    search_after: Optional[List[Any]] = None

    while len(uuids) < max_records:
        body: Dict[str, Any] = {
            "_source": {"includes": [uuid_field, timestamp_field]},
            "size": min(page_size, max_records - len(uuids)),
            "query": {
                "range": {
                    timestamp_field: {
                        "gte": to_es_datetime(start),
                        "lt": to_es_datetime(end),
                    }
                }
            },
            "sort": [
                {timestamp_field: "asc"},
                {"_doc": "asc"},
            ],
        }
        if search_after:
            body["search_after"] = search_after

        response = search_fn(instance["base_url"], auth_headers, index_name, body)
        if not response.get("ok"):
            return response

        hits = response.get("data", {}).get("hits", {}).get("hits", [])
        if not hits:
            break

        for hit in hits:
            source = hit.get("_source", {})
            uuid_value = source.get(uuid_field)
            if uuid_value is not None:
                uuids.append(str(uuid_value))
            if len(uuids) >= max_records:
                break

        search_after = hits[-1].get("sort")
        if not search_after:
            break

    return {
        "ok": True,
        "status_code": 200,
        "uuids": uuids,
        "uuid_limit_reached": len(uuids) >= max_records,
    }


def build_index_activity_report(
    instances: List[Dict[str, str]],
    indices_by_instance: Dict[str, List[Dict[str, Any]]],
    period_label: str,
    start: datetime,
    end: datetime,
    include_uuids: bool,
    count_fn: Callable[[Dict[str, str], str], Dict[str, Any]],
    fetch_uuids_fn: Callable[[Dict[str, str], str], Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    report_date = datetime.utcnow().date().isoformat()

    for instance in instances:
        instance_name = instance["name"]
        base_url = instance["base_url"]
        for index_info in indices_by_instance.get(base_url, []):
            index_name = index_info.get("index", "")
            base_row = {
                "report_date": report_date,
                "instance_name": instance_name,
                "base_url": base_url,
                "index_name": index_name,
                "period_label": period_label,
                "start_datetime": to_es_datetime(start),
                "end_datetime": to_es_datetime(end),
            }

            count_resp = count_fn(instance, index_name)
            if not count_resp.get("ok"):
                error_text = str(count_resp.get("message", "Failed to count index activity"))
                rows.append(
                    {
                        **base_row,
                        "activity_count": 0,
                        "has_activity": False,
                        "error": error_text,
                        **({"call_uuid": ""} if include_uuids else {}),
                    }
                )
                errors.append(
                    {
                        "instance_name": instance_name,
                        "base_url": base_url,
                        "index_name": index_name,
                        "error": error_text,
                    }
                )
                continue

            activity_count = int(count_resp.get("count", 0))
            has_activity = activity_count > 0

            if not include_uuids:
                rows.append(
                    {
                        **base_row,
                        "activity_count": activity_count,
                        "has_activity": has_activity,
                    }
                )
                continue

            uuids_resp = fetch_uuids_fn(instance, index_name)
            if not uuids_resp.get("ok"):
                error_text = str(uuids_resp.get("message", "Failed to fetch UUIDs"))
                rows.append(
                    {
                        **base_row,
                        "activity_count": activity_count,
                        "has_activity": has_activity,
                        "call_uuid": "",
                        "error": error_text,
                    }
                )
                errors.append(
                    {
                        "instance_name": instance_name,
                        "base_url": base_url,
                        "index_name": index_name,
                        "error": error_text,
                    }
                )
                continue

            uuids = uuids_resp.get("uuids", [])
            if not uuids:
                rows.append(
                    {
                        **base_row,
                        "activity_count": activity_count,
                        "has_activity": False,
                        "call_uuid": "",
                    }
                )
                continue

            for uuid_value in uuids:
                rows.append(
                    {
                        **base_row,
                        "activity_count": activity_count,
                        "has_activity": has_activity,
                        "call_uuid": uuid_value,
                    }
                )

    return rows, errors
