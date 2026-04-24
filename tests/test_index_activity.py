from datetime import date, datetime
import unittest

from index_activity import build_date_range, build_index_activity_report


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


if __name__ == "__main__":
    unittest.main()
