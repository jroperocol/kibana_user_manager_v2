from field_limit_audit import (
    build_instance_summary,
    build_update_preview,
    extract_data_views,
    extract_indices_from_cat,
    merge_template_limit,
    parse_total_fields_limit,
)


def test_parse_flat_total_fields_limit():
    result = parse_total_fields_limit({"idx": {"settings": {"index.mapping.total_fields.limit": "1000"}}}, "idx")
    assert result["total_fields_limit"] == 1000
    assert result["default_assumed"] is False
    assert result["status"] == "ok"


def test_missing_setting_is_default():
    result = parse_total_fields_limit({"idx": {"settings": {}}}, "idx")
    assert result["total_fields_limit"] == 1000
    assert result["default_assumed"] is True
    assert result["above_1000"] is False
    assert result["status"] == "default_assumed"


def test_above_1000_detected_as_workaround():
    result = parse_total_fields_limit({"settings": {"index": {"mapping": {"total_fields": {"limit": "2000"}}}}})
    assert result["total_fields_limit"] == 2000
    assert result["above_1000"] is True
    assert result["status"] == "above_1000"


def test_non_numeric_is_error():
    result = parse_total_fields_limit({"settings": {"index.mapping.total_fields.limit": "many"}})
    assert result["status"] == "error"
    assert "Non-numeric" in result["error_message"]


def test_instance_summary_aggregation():
    rows = [
        {"index_name": "a", "total_fields_limit": 1000, "default_assumed": True, "above_1000": False, "status": "default_assumed"},
        {"index_name": "b", "total_fields_limit": 1500, "default_assumed": False, "above_1000": True, "status": "above_1000"},
        {"index_name": "c", "total_fields_limit": "", "default_assumed": False, "above_1000": False, "status": "error"},
    ]
    summary = build_instance_summary({"name": "inst", "base_url": "http://x"}, rows)
    assert summary["workaround_detected"] is True
    assert summary["indices_checked"] == 2
    assert summary["indices_above_1000"] == 1
    assert summary["indices_failed"] == 1
    assert summary["status"] == "partial"


def test_extract_data_views_current_shape_compatibility():
    views = extract_data_views({"data_views": [{"id": "1", "title": "logs-*"}, {"id": "2", "name": "metrics-*"}]})
    assert views == [
        {"id": "1", "title": "logs-*", "name": "logs-*"},
        {"id": "2", "title": "metrics-*", "name": "metrics-*"},
    ]


def test_extract_data_views_saved_objects_shape_compatibility():
    views = extract_data_views({"saved_objects": [{"id": "abc", "attributes": {"title": "legacy-*"}}]}, legacy=True)
    assert views == [{"id": "abc", "title": "legacy-*", "name": "legacy-*"}]


def test_extract_indices_from_cat():
    assert extract_indices_from_cat([{"index": "a"}, {"index": "b"}, {"health": "green"}]) == ["a", "b"]


def test_build_update_preview_default_mode():
    rows = [
        {"instance": "i", "base_url": "http://x", "index_name": "a", "total_fields_limit": 1000, "default_assumed": True, "status": "default_assumed", "template_name": "t", "template_limit": ""},
        {"instance": "i", "base_url": "http://x", "index_name": "b", "total_fields_limit": 3000, "default_assumed": False, "status": "above_1000", "template_name": "", "template_limit": ""},
    ]
    preview = build_update_preview(rows, ["i"], set(), "default", 2000, True, True)
    assert len(preview) == 1
    assert preview[0]["index_name"] == "a"
    assert preview[0]["action"] == "dry_run"
    assert preview[0]["update_required"] is True
    assert preview[0]["template_new_limit"] == 2000


def test_merge_template_limit_preserves_composable_body():
    body = {"index_template": {"index_patterns": ["logs-*"], "template": {"settings": {"number_of_shards": 1}, "mappings": {"properties": {}}}, "priority": 1}}
    merged = merge_template_limit(body, "composable", 2000)
    assert merged["template"]["settings"]["number_of_shards"] == 1
    assert merged["template"]["settings"]["index.mapping.total_fields.limit"] == 2000
    assert merged["priority"] == 1


def test_merge_template_limit_handles_named_composable_get_response():
    body = {"index_templates": [{"name": "logs", "index_template": {"index_patterns": ["logs-*"], "template": {"settings": {}}}}]}
    merged = merge_template_limit(body, "composable", 2500)
    assert merged["index_patterns"] == ["logs-*"]
    assert merged["template"]["settings"]["index.mapping.total_fields.limit"] == 2500
