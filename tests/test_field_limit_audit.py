from io import BytesIO

import pandas as pd

from field_limit_audit import (
    build_field_limit_excel,
    build_instance_summary,
    build_update_preview,
    extract_data_views,
    extract_indices_from_cat,
    merge_template_limit,
    parse_total_fields_limit,
)


def test_app_import_contract_names_exist():
    import field_limit_audit

    expected = [
        "build_field_limit_excel",
        "build_instance_summary",
        "build_update_preview",
        "encoded_path",
        "extract_indices_from_cat",
        "extract_templates",
        "match_template",
        "merge_template_limit",
        "now_ts",
        "parse_total_fields_limit",
        "readonly_get",
        "safe_put_index_field_limit",
        "safe_put_template",
    ]
    assert [name for name in expected if not hasattr(field_limit_audit, name)] == []


def test_parse_flat_total_fields_limit():
    result = parse_total_fields_limit({"idx": {"settings": {"index.mapping.total_fields.limit": "1000"}}}, "idx")
    assert result["configured_limit"] == "1000"
    assert result["configured_limit_status"] == "configured"
    assert result["effective_default"] is None
    assert result["status"] == "configured_1000_or_less"


def test_missing_setting_is_default():
    result = parse_total_fields_limit({"idx": {"settings": {}}}, "idx")
    assert result["configured_limit"] is None
    assert result["configured_limit_status"] == "not_configured"
    assert result["effective_default"] == 1000
    assert result["above_1000"] is False
    assert result["status"] == "not_configured"


def test_above_1000_detected_as_workaround():
    result = parse_total_fields_limit({"settings": {"index": {"mapping": {"total_fields": {"limit": "2000"}}}}})
    assert result["configured_limit"] == "2000"
    assert result["above_1000"] is True
    assert result["raw_setting_path"] == "settings.index.mapping.total_fields.limit"
    assert result["status"] == "configured_above_1000"


def test_non_numeric_is_error():
    result = parse_total_fields_limit({"settings": {"index.mapping.total_fields.limit": "many"}})
    assert result["status"] == "error"
    assert "Non-numeric" in result["error_message"]


def test_instance_summary_aggregation():
    rows = [
        {"index_name": "a", "configured_limit": None, "configured_limit_status": "not_configured", "effective_default": 1000, "above_1000": False, "status": "not_configured"},
        {"index_name": "b", "configured_limit": "1500", "configured_limit_status": "configured", "effective_default": None, "above_1000": True, "status": "configured_above_1000"},
        {"index_name": "c", "configured_limit": None, "configured_limit_status": "request_error", "effective_default": None, "above_1000": False, "status": "error"},
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
        {"instance": "i", "base_url": "http://x", "index_name": "a", "configured_limit": None, "configured_limit_status": "not_configured", "effective_default": 1000, "status": "not_configured", "template_name": "t", "template_limit": ""},
        {"instance": "i", "base_url": "http://x", "index_name": "b", "configured_limit": "3000", "configured_limit_status": "configured", "effective_default": None, "status": "configured_above_1000", "template_name": "", "template_limit": ""},
    ]
    preview = build_update_preview(rows, ["i"], set(), "default", 2000, True, True)
    assert len(preview) == 1
    assert preview[0]["index_name"] == "a"
    assert preview[0]["action"] == "dry_run"
    assert preview[0]["configured_limit"] is None
    assert preview[0]["effective_default"] == 1000
    assert preview[0]["reason"] == "not_configured_effective_default_1000"
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


def test_build_field_limit_excel_uses_simplified_sheet_names():
    data = build_field_limit_excel(
        [{"instance": "i", "current_effective_limit": 1000}],
        [{"instance": "i", "index_name": "a"}],
        [],
        [],
        [{"instance": "i", "new_limit": 2000}],
        [{"instance": "i", "updated": False}],
        [],
        [],
    )
    workbook = pd.ExcelFile(BytesIO(data))
    assert workbook.sheet_names == ["instance_limits", "update_preview", "update_results", "technical_details"]
