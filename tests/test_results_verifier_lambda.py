import json
import logging
import os
from pathlib import Path
from unittest import TestCase, mock

import boto3
from moto import mock_s3, mock_sns

from src.results_verifier_lambda import event_handler


class TestResultsVerifier(TestCase):

    @mock_s3
    def test_entry_point(self):
        self.setUp_s3()
        sns_topic_arn = self.setup_sns()
        event = self.get_event()
        with mock.patch.dict(os.environ, {"SNS_TOPIC": sns_topic_arn, "AWS_REGION": "eu-west-2"}):
            result = event_handler.handler(event, None)
            self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)

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

    def test_generate_message_payload_missing_exports(self):
        result = event_handler.generate_message_payload(5, 100)
        expected_result = {
            'severity': 'Critical',
            'notification_type': 'Error',
            'slack_username': 'AWS Lambda Notification',
            'title_text': 'Kafka reconciliation results',
            'custom_elements': [
                {'key': 'Exported count', 'value': '100'},
                {'key': 'Missing exports count', 'value': '5'}
            ]
        }
        self.assertEqual(result, expected_result)

    def test_generate_message_payload_no_missing_exports(self):
        result = event_handler.generate_message_payload(0, 50)
        expected_result = {
            'severity': 'High',
            'notification_type': 'Information',
            'slack_username': 'AWS Lambda Notification',
            'title_text': 'Kafka reconciliation results',
            'custom_elements': [
                {'key': 'Exported count', 'value': '50'}
            ]
        }
        self.assertEqual(result, expected_result)

    @staticmethod
    def get_event():
        return {
            "Records": [
                {
                    "eventVersion": "2.0",
                    "eventSource": "aws:s3",
                    "awsRegion": "eu-west-2",
                    "eventTime": "1970-01-01T00:00:00.000Z",
                    "eventName": "ObjectCreated:Put",
                    "userIdentity": {
                        "principalId": "EXAMPLE"
                    },
                    "requestParameters": {
                        "sourceIPAddress": "127.0.0.1"
                    },
                    "responseElements": {
                        "x-amz-request-id": "EXAMPLE123456789",
                        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
                    },
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "testConfigRule",
                        "bucket": {
                            "name": "results_verifier_test",
                            "ownerIdentity": {
                                "principalId": "EXAMPLE"
                            },
                            "arn": "arn:aws:s3:::example-bucket"
                        },
                        "object": {
                            "key": "results/query_results.json",
                            "size": 1024,
                            "eTag": "0123456789abcdef0123456789abcdef",
                            "sequencer": "0A1B2C3D4E5F678901"
                        }
                    }
                }
            ]
        }

    @mock_s3
    def setUp_s3(self):
        s3_client = boto3.client("s3")
        s3_client.create_bucket(
            Bucket="results_verifier_test",
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            },
        )
        path = Path(os.getcwd())
        results_json_path = f"{path.parent.absolute()}/resources/results.json"
        with open(results_json_path) as f:
            json_record = json.load(f)
        s3_client.put_object(
            Body=json.dumps(json_record),
            Bucket="results_verifier_test",
            Key="results/query_results.json",

        )
        event_handler.s3_client = s3_client

    @mock_sns
    def setup_sns(self):
        sns_client = boto3.client(service_name="sns", region_name="eu-west-2")
        sns_client.create_topic(
            Name="monitoring_topic", Attributes={"DisplayName": "test-topic"}
        )
        topics_json = sns_client.list_topics()
        event_handler.sns_client = sns_client
        return topics_json["Topics"][0]["TopicArn"]

    def setUp(self):
        event_handler.logger = logging.getLogger()
