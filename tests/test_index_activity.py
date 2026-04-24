from datetime import date, datetime
import unittest

from index_activity import (
    MAX_UUID_RECORDS,
    INDEX_PATTERN_SUFFIX,
    build_activity_agg_query,
    build_activity_rows_for_instance,
    build_count_query,
    build_error_row,
    build_date_range,
    build_index_activity_report,
    build_search_url,
    build_uuid_query,
    derive_search_pattern_from_index,
    extract_total_hits,
    extract_uuid_rows,
    fetch_index_uuids,
    filter_indices,
    list_indices,
    build_period_range,
    parse_activity_buckets,
)


class TestBuildDateRange(unittest.TestCase):
    def test_today(self):
        now = datetime(2026, 4, 24, 15, 30, 0)
        label, start, end = build_date_range("Today", now=now)
        self.assertEqual(label, "Today")
        self.assertEqual(start, datetime(2026, 4, 24, 0, 0, 0))
        self.assertEqual(end, now)

    def test_last_24_hours(self):
        now = datetime(2026, 4, 24, 15, 30, 0)
        _, start, end = build_date_range("Last 24 hours", now=now)
        self.assertEqual(start, datetime(2026, 4, 23, 15, 30, 0))
        self.assertEqual(end, now)

    def test_last_7_30_60_90_days(self):
        now = datetime(2026, 4, 24, 15, 30, 0)
        expected = {
            "Last 7 days": datetime(2026, 4, 17, 15, 30, 0),
            "Last 30 days": datetime(2026, 3, 25, 15, 30, 0),
            "Last 60 days": datetime(2026, 2, 23, 15, 30, 0),
            "Last 90 days": datetime(2026, 1, 24, 15, 30, 0),
        }
        for period, expected_start in expected.items():
            _, start, end = build_date_range(period, now=now)
            self.assertEqual(start, expected_start)
            self.assertEqual(end, now)

    def test_custom(self):
        label, start, end = build_date_range("Custom date range", custom_start=date(2026, 4, 1), custom_end=date(2026, 4, 2))
        self.assertEqual(label, "Custom date range")
        self.assertEqual(start, datetime(2026, 4, 1, 0, 0, 0))
        self.assertEqual(end.date(), date(2026, 4, 2))


class TestBuildIndexActivityReport(unittest.TestCase):
    def setUp(self):
        self.instances = [{"name": "inst1", "base_url": "https://example"}]
        self.indices = {"https://example": [{"index": "calls-001"}]}
        self.start = datetime(2026, 4, 1, 0, 0, 0)
        self.end = datetime(2026, 4, 2, 0, 0, 0)

    def test_rows_without_uuid(self):
        def count_fn(_instance, _index):
            return {"ok": True, "count": 3}

        rows, errors = build_index_activity_report(
            self.instances,
            self.indices,
            "Last 24 hours",
            self.start,
            self.end,
            False,
            count_fn,
            lambda _i, _n: {"ok": True, "uuids": []},
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["activity_count"], 3)
        self.assertTrue(rows[0]["has_activity"])
        self.assertEqual(errors, [])

    def test_rows_with_uuid(self):
        def count_fn(_instance, _index):
            return {"ok": True, "count": 2}

        def uuid_fn(_instance, _index):
            return {"ok": True, "uuids": ["u1", "u2"]}

        rows, errors = build_index_activity_report(
            self.instances,
            self.indices,
            "Last 24 hours",
            self.start,
            self.end,
            True,
            count_fn,
            uuid_fn,
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["call_uuid"], "u1")
        self.assertEqual(rows[1]["call_uuid"], "u2")
        self.assertEqual(errors, [])

    def test_zero_activity_with_uuid_enabled(self):
        def count_fn(_instance, _index):
            return {"ok": True, "count": 0}

        def uuid_fn(_instance, _index):
            return {"ok": True, "uuids": []}

        rows, _ = build_index_activity_report(
            self.instances,
            self.indices,
            "Last 24 hours",
            self.start,
            self.end,
            True,
            count_fn,
            uuid_fn,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["activity_count"], 0)
        self.assertFalse(rows[0]["has_activity"])
        self.assertEqual(rows[0]["call_uuid"], "")

    def test_failed_index_query(self):
        def count_fn(_instance, _index):
            return {"ok": False, "message": "boom"}

        rows, errors = build_index_activity_report(
            self.instances,
            self.indices,
            "Last 24 hours",
            self.start,
            self.end,
            False,
            count_fn,
            lambda _i, _n: {"ok": True, "uuids": []},
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["activity_count"], 0)
        self.assertFalse(rows[0]["has_activity"])
        self.assertIn("boom", rows[0]["error"])
        self.assertEqual(len(errors), 1)


class TestIndexFilteringAndPattern(unittest.TestCase):
    def test_build_search_url(self):
        url = build_search_url("https://example:9243/", "40506_broadvoice_collections_internal*")
        self.assertEqual(url, "https://example:9243/40506_broadvoice_collections_internal*/_search")

    def test_build_count_query_last_90_days(self):
        gte, lt = build_period_range("Last 90 days")
        body = build_count_query(gte, lt)
        self.assertEqual(body["query"]["range"]["timestamp"]["gte"], "now-90d")
        self.assertEqual(body["query"]["range"]["timestamp"]["lt"], "now")

    def test_build_uuid_query_includes_only_call_uuid_and_timestamp(self):
        body = build_uuid_query("now-90d", "now")
        self.assertEqual(body["_source"]["includes"], ["call.uuid", "timestamp"])

    def test_derive_search_pattern_from_index(self):
        self.assertEqual(
            derive_search_pattern_from_index("40506_broadvoice_collections_internal_ivrs-202604"),
            "40506_broadvoice_collections_internal*",
        )
        self.assertEqual(derive_search_pattern_from_index("other"), INDEX_PATTERN_SUFFIX)

    def test_report_parsing_total_and_uuid(self):
        resp = {
            "data": {
                "hits": {
                    "total": {"value": 2},
                    "hits": [
                        {"_source": {"call": {"uuid": "u1"}, "timestamp": "2026-04-01T00:00:00"}},
                        {"_source": {"call": {"uuid": "u2"}, "timestamp": "2026-04-01T00:01:00"}},
                    ],
                }
            }
        }
        self.assertEqual(extract_total_hits(resp), 2)
        parsed = extract_uuid_rows(resp)
        self.assertEqual(parsed[0]["call_uuid"], "u1")
        self.assertEqual(parsed[1]["call_uuid"], "u2")

    def test_filter_indices_excludes_system_by_default(self):
        indices = [
            {"index": ".kibana_1"},
            {"index": ".ds-logs"},
            {"index": "foo_ivrs-2026.01.01"},
        ]
        filtered = filter_indices(indices, include_system=False)
        self.assertEqual(filtered, [{"index": "foo_ivrs-2026.01.01"}])

    def test_list_indices_uses_pattern(self):
        captured = {}

        def fake_fetch(base_url, auth_headers, pattern):
            captured["pattern"] = pattern
            return {"ok": True, "data": []}

        list_indices({"name": "x", "base_url": "https://example"}, {"Authorization": "x"}, fake_fetch, "*_ivrs-*")
        self.assertEqual(captured["pattern"], "*_ivrs-*")

    def test_report_uses_only_filtered_indices(self):
        instances = [{"name": "inst1", "base_url": "https://example"}]
        indices_by_instance = {"https://example": [{"index": "ivr-1"}]}

        seen = []

        def count_fn(_instance, index_name):
            seen.append(index_name)
            return {"ok": True, "count": 1}

        rows, _ = build_index_activity_report(
            instances,
            indices_by_instance,
            "Last 24 hours",
            datetime(2026, 4, 1, 0, 0, 0),
            datetime(2026, 4, 2, 0, 0, 0),
            False,
            count_fn,
            lambda _i, _n: {"ok": True, "uuids": []},
        )
        self.assertEqual(seen, ["ivr-1"])
        self.assertEqual(len(rows), 1)

    def test_failed_instance_does_not_stop_report_processing(self):
        instances = [{"name": "a", "base_url": "https://a"}, {"name": "b", "base_url": "https://b"}]
        results = []
        for instance in instances:
            if instance["name"] == "a":
                results.append({"instance_name": "a", "error": "boom"})
                continue
            results.append({"instance_name": "b", "activity_count": 5})
        self.assertEqual(len(results), 2)
        self.assertEqual(results[1]["instance_name"], "b")

    def test_uuid_limit_respected(self):
        def fake_search(_base_url, _headers, _index, body):
            size = body["size"]
            hits = []
            for i in range(size):
                idx = i + (0 if "search_after" not in body else body["search_after"][1] + 1)
                hits.append({"_source": {"uuid": f"u{idx}"}, "sort": ["2026-04-01T00:00:00Z", idx]})
            return {"ok": True, "data": {"hits": {"hits": hits}}}

        result = fetch_index_uuids(
            {"name": "inst1", "base_url": "https://example"},
            "ivr-index",
            datetime(2026, 4, 1, 0, 0, 0),
            datetime(2026, 4, 2, 0, 0, 0),
            "timestamp",
            "uuid",
            {"Authorization": "x"},
            fake_search,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(len(result["uuids"]), MAX_UUID_RECORDS)

    def test_uuid_limit_respected_with_custom_max(self):
        def fake_search(_base_url, _headers, _index, body):
            size = body["size"]
            hits = [{"_source": {"uuid": f"u{i}"}, "sort": ["2026-04-01T00:00:00Z", i]} for i in range(size)]
            return {"ok": True, "data": {"hits": {"hits": hits}}}

        result = fetch_index_uuids(
            {"name": "inst1", "base_url": "https://example"},
            "ivr-index",
            datetime(2026, 4, 1, 0, 0, 0),
            datetime(2026, 4, 2, 0, 0, 0),
            "timestamp",
            "uuid",
            {"Authorization": "x"},
            fake_search,
            max_records=1000,
            page_size=1000,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(len(result["uuids"]), 1000)
        self.assertTrue(result["uuid_limit_reached"])

    def test_no_auto_execution_without_button_intent(self):
        calls = {"count": 0}

        def count_fn(_instance, _index):
            calls["count"] += 1
            return {"ok": True, "count": 1}

        self.assertEqual(calls["count"], 0)

    def test_build_activity_agg_query(self):
        body = build_activity_agg_query(datetime(2026, 4, 1, 0, 0, 0), datetime(2026, 4, 2, 0, 0, 0), "timestamp")
        self.assertEqual(body["size"], 0)
        self.assertTrue(body["track_total_hits"])
        self.assertEqual(body["aggs"]["by_index"]["terms"]["field"], "_index")
        self.assertEqual(body["aggs"]["by_index"]["terms"]["size"], 10000)
        self.assertIn("lt", body["query"]["range"]["timestamp"])

    def test_parse_buckets_and_zero_merge(self):
        resp = {
            "data": {
                "aggregations": {
                    "by_index": {
                        "buckets": [
                            {"key": "ivr-a", "doc_count": 5},
                            {"key": ".kibana", "doc_count": 4},
                        ]
                    }
                }
            }
        }
        counts = parse_activity_buckets(resp, include_system=False)
        self.assertEqual(counts, {"ivr-a": 5})

        rows = build_activity_rows_for_instance(
            {"name": "inst", "base_url": "https://e"},
            "Last 24 hours",
            datetime(2026, 4, 1, 0, 0, 0),
            datetime(2026, 4, 2, 0, 0, 0),
            counts,
            loaded_indices=["ivr-a", "ivr-b"],
        )
        mapped = {row["index_name"]: row["activity_count"] for row in rows}
        self.assertEqual(mapped["ivr-a"], 5)
        self.assertEqual(mapped["ivr-b"], 0)

    def test_timeout_error_row_behavior(self):
        row = build_error_row(
            {"name": "inst", "base_url": "https://e"},
            "Last 24 hours",
            datetime(2026, 4, 1, 0, 0, 0),
            datetime(2026, 4, 2, 0, 0, 0),
            "Request timed out",
        )
        self.assertEqual(row["activity_count"], 0)
        self.assertFalse(row["has_activity"])
        self.assertIn("timed out", row["error"])

    def test_uuid_mode_only_for_positive_activity_indices(self):
        rows = [
            {"index_name": "a", "activity_count": 0},
            {"index_name": "b", "activity_count": 2},
        ]
        to_fetch = [r["index_name"] for r in rows if int(r.get("activity_count", 0)) > 0]
        self.assertEqual(to_fetch, ["b"])


if __name__ == "__main__":
    unittest.main()
