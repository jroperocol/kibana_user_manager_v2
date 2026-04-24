import json
import unittest
from unittest.mock import patch

import elastic_client


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, text=None, reason=""):
        self.status_code = status_code
        self._payload = payload
        self.reason = reason
        if text is None and payload is not None:
            text = json.dumps(payload)
        self.text = text or ""

    def json(self):
        if self._payload is None:
            raise ValueError("No JSON")
        return self._payload


class TestElasticClientIndexHelpers(unittest.TestCase):
    @patch("elastic_client.requests.request")
    def test_list_indices_normalizes_response(self, mock_request):
        mock_request.return_value = _FakeResponse(
            payload=[
                {
                    "index": "calls-0001",
                    "health": "green",
                    "status": "open",
                    "docs.count": "10",
                    "store.size": "5mb",
                    "extra": "ignore",
                }
            ]
        )

        resp = elastic_client.list_indices("https://example", {"Authorization": "x"})

        self.assertTrue(resp["ok"])
        self.assertEqual(
            resp["data"][0],
            {
                "index": "calls-0001",
                "health": "green",
                "status": "open",
                "docs.count": "10",
                "store.size": "5mb",
            },
        )
        called_url = mock_request.call_args.kwargs["url"]
        self.assertTrue(called_url.endswith("/_cat/indices/*"))

    @patch("elastic_client.requests.request")
    def test_list_indices_returns_clear_error(self, mock_request):
        mock_request.return_value = _FakeResponse(status_code=500, text="boom")

        resp = elastic_client.list_indices("https://example", {"Authorization": "x"})

        self.assertFalse(resp["ok"])
        self.assertIn("Failed to list indices", resp["message"])

    @patch("elastic_client.requests.request")
    def test_list_indices_custom_pattern(self, mock_request):
        mock_request.return_value = _FakeResponse(payload=[])
        elastic_client.list_indices("https://example", {"Authorization": "x"}, pattern="*_ivrs-*")
        called_url = mock_request.call_args.kwargs["url"]
        self.assertTrue(called_url.endswith("/_cat/indices/*_ivrs-*"))

    @patch("elastic_client.requests.request")
    def test_search_index_returns_clear_error(self, mock_request):
        mock_request.return_value = _FakeResponse(status_code=404, text="not found")

        resp = elastic_client.search_index("https://example", {"Authorization": "x"}, "missing-index", {"size": 0})

        self.assertFalse(resp["ok"])
        self.assertIn("Failed to search index 'missing-index'", resp["message"])


if __name__ == "__main__":
    unittest.main()
