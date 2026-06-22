from field_limit_audit import build_instance_summary, extract_data_views, parse_total_fields_limit


def test_parse_flat_total_fields_limit():
    result = parse_total_fields_limit({"idx": {"settings": {"index.mapping.total_fields.limit": "1000"}}}, "idx")
    assert result["total_fields_limit"] == 1000
    assert result["default_assumed"] is False
    assert result["status"] == "default_or_missing"


def test_missing_setting_is_default():
    result = parse_total_fields_limit({"idx": {"settings": {}}}, "idx")
    assert result["total_fields_limit"] == 1000
    assert result["default_assumed"] is True
    assert result["above_1000"] is False


def test_above_1000_detected_as_workaround():
    result = parse_total_fields_limit({"settings": {"index": {"mapping": {"total_fields": {"limit": "2000"}}}}})
    assert result["total_fields_limit"] == 2000
    assert result["above_1000"] is True
    assert result["status"] == "workaround_detected"


def test_non_numeric_is_error():
    result = parse_total_fields_limit({"settings": {"index.mapping.total_fields.limit": "many"}})
    assert result["status"] == "error"
    assert "Non-numeric" in result["error_message"]


def test_instance_summary_aggregation():
    rows = [
        {"matched_index": "a", "total_fields_limit": 1000, "above_1000": False, "status": "default_or_missing"},
        {"matched_index": "b", "total_fields_limit": 1500, "above_1000": True, "status": "workaround_detected"},
        {"matched_index": "c", "total_fields_limit": "", "above_1000": False, "status": "error"},
    ]
    summary = build_instance_summary({"name": "inst", "base_url": "http://x"}, rows, 2)
    assert summary["workaround_detected"] is True
    assert summary["indices_checked"] == 2
    assert summary["indices_above_1000"] == 1
    assert summary["indices_failed"] == 1
    assert summary["status"] == "partial"


def test_extract_data_views_current_shape():
    views = extract_data_views({"data_views": [{"id": "1", "title": "logs-*"}, {"id": "2", "name": "metrics-*"}]})
    assert views == [
        {"id": "1", "title": "logs-*", "name": "logs-*"},
        {"id": "2", "title": "metrics-*", "name": "metrics-*"},
    ]


def test_extract_data_views_saved_objects_shape():
    views = extract_data_views({"saved_objects": [{"id": "abc", "attributes": {"title": "legacy-*"}}]}, legacy=True)
    assert views == [{"id": "abc", "title": "legacy-*", "name": "legacy-*"}]
