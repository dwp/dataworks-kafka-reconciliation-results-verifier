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

        count = event_handler.count_missing_exports(json_record)
        self.assertEqual(count, 8)

    def test_count_missing_exports_null(self):
        null_json_record = {
            "query_results": [
                {
                    "query_details": {
                        "enabled": 'true',
                        "query_name": "Missing exported totals",
                        "query_type": "main",
                        "query_description": "Shows stats on the total amount of data missing from export",
                        "query_file": "missing_exported_totals.sql",
                        "results_file": "missing_exported_totals.csv",
                        "show_column_names": 'true',
                        "order": 5
                    },
                    "query_results": [
                        {
                            "missing_exported_count": "null"
                        }
                    ]
                }

            ]
        }

        count = event_handler.count_missing_exports(null_json_record)
        self.assertEqual(count, 0)

    def setUp(self):
        event_handler.logger = logging.getLogger()
