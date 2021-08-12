import json
import logging
import os
import unittest
from pathlib import Path

from src.results_verifier_lambda import event_handler


class TestResultsVerifier(unittest.TestCase):
    def test_count_missing_exports(self):
        path = Path(os.getcwd())
        results_json_path = f"{path.parent.absolute()}/resources/results.json"
        with open(results_json_path) as f:
            json_record = json.load(f)

        missing_export_count, export_count = event_handler.get_counts(json_record)
        self.assertEqual(missing_export_count, 8)
        self.assertEqual(export_count, 5)

    def test_count_missing_exports_null(self):
        null_json_record = {
            "query_results": [
                {
                    "query_details": {
                        "query_name": "Missing exported totals",
                    },
                    "query_results": [
                        {
                            "missing_exported_count": "null"
                        }
                    ]
                },
                {
                    "query_details": {
                        "query_name": "Export totals",
                    },
                    "query_results": [
                        {

                            "exported_count": "null"
                        }
                    ]
                }

            ]
        }

        missing_export_count, export_count = event_handler.get_counts(null_json_record)
        self.assertEqual(missing_export_count, 0)
        self.assertEqual(export_count, 0)

    def setUp(self):
        event_handler.logger = logging.getLogger()
