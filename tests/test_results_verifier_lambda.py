import json
import logging
import os
from pathlib import Path
from unittest import TestCase, mock

import boto3
from moto import mock_s3, mock_sns, mock_sqs
from results_verifier_lambda import event_handler


class TestResultsVerifier(TestCase):
    @mock_s3
    @mock_sqs
    def test_entry_point(self):
        path = Path(os.getcwd())
        results_json_path = f"{path.parent.absolute()}/dataworks-kafka-reconciliation-results-verifier/resources/results.json"
        with open(results_json_path) as f:
            json_record = json.load(f)
        self.setUp_s3(json_record)
        sqs_client = boto3.client("sqs", region_name="eu-west-2")
        sqs_url = sqs_client.create_queue(QueueName="test")["QueueUrl"]
        sqs_arn = sqs_client.get_queue_attributes(
            QueueUrl=sqs_url, AttributeNames=["All"]
        )["Attributes"]["QueueArn"]
        sns_topic_arn = self.setup_sns(sqs_arn)

        event = self.get_event()
        with mock.patch.dict(
            os.environ, {"SNS_TOPIC": sns_topic_arn, "AWS_REGION": "eu-west-2"}
        ):
            event_handler.handler(event, None)
            messages = sqs_client.receive_message(
                QueueUrl=sqs_url, MaxNumberOfMessages=10
            )["Messages"]
            self.assertEqual(1, len(messages))
            message_body = json.loads(messages[0]["Body"])
            message = json.loads(message_body["Message"])

            expected_message = {
                "severity": "Critical",
                "notification_type": "Error",
                "slack_username": "AWS Lambda Notification",
                "title_text": "Kafka reconciliation - missing records",
                "custom_elements": [
                    {"key": "Exported count", "value": "5"},
                    {"key": "Missing exports count", "value": "8"},
                    {
                        "key": "S3-Location",
                        "value": f"s3://manifest-bucket/results/query_results.json",
                    },
                ],
            }
            print("£££££££££")
            print(message)
            print(expected_message)
            self.assertEqual(message, expected_message)

    @mock_s3
    @mock_sqs
    def test_entry_point_null_results(self):
        null_json_record = self.get_record_containing_nulls()
        self.setUp_s3(null_json_record)
        sqs_client = boto3.client("sqs", region_name="eu-west-2")
        sqs_url = sqs_client.create_queue(QueueName="test")["QueueUrl"]
        sqs_arn = sqs_client.get_queue_attributes(
            QueueUrl=sqs_url, AttributeNames=["All"]
        )["Attributes"]["QueueArn"]

        sns_topic_arn = self.setup_sns(sqs_arn)

        event = self.get_event()
        with mock.patch.dict(
            os.environ, {"SNS_TOPIC": sns_topic_arn, "AWS_REGION": "eu-west-2"}
        ):
            event_handler.handler(event, None)
            messages = sqs_client.receive_message(
                QueueUrl=sqs_url, MaxNumberOfMessages=10
            )["Messages"]
            self.assertEqual(1, len(messages))
            message_body = json.loads(messages[0]["Body"])
            message = json.loads(message_body["Message"])

            expected_message = {
                "severity": "High",
                "notification_type": "Information",
                "slack_username": "AWS Lambda Notification",
                "title_text": "Kafka reconciliation successful",
                "custom_elements": [{"key": "Exported count", "value": "0"}],
            }

            self.assertEqual(message, expected_message)

    def test_count_missing_exports(self):
        path = Path(os.getcwd())
        results_json_path = f"{path.parent.absolute()}/dataworks-kafka-reconciliation-results-verifier/resources/results.json"
        with open(results_json_path) as f:
            json_record = json.load(f)

        missing_export_count, export_count = event_handler.get_counts(json_record)
        self.assertEqual(missing_export_count, 8)
        self.assertEqual(export_count, 5)

    def test_count_missing_exports_null(self):
        null_json_record = self.get_record_containing_nulls()
        missing_export_count, export_count = event_handler.get_counts(null_json_record)
        self.assertEqual(missing_export_count, 0)
        self.assertEqual(export_count, 0)

    def test_generate_message_payload_missing_exports(self):
        result = event_handler.generate_message_payload(
            5, 100, "results/query_results.json"
        )
        expected_result = {
            "severity": "Critical",
            "notification_type": "Error",
            "slack_username": "AWS Lambda Notification",
            "title_text": "Kafka reconciliation - missing records",
            "custom_elements": [
                {"key": "Exported count", "value": "100"},
                {"key": "Missing exports count", "value": "5"},
                {
                    "key": "S3-Location",
                    "value": f"s3://manifest-bucket/results/query_results.json",
                },
            ],
        }
        print("£££££££££")
        print(result)
        print(expected_result)
        self.assertEqual(result, expected_result)

    def test_generate_message_payload_no_missing_exports(self):
        result = event_handler.generate_message_payload(
            0, 50, "results/query_results.json"
        )
        expected_result = {
            "severity": "High",
            "notification_type": "Information",
            "slack_username": "AWS Lambda Notification",
            "title_text": "Kafka reconciliation successful",
            "custom_elements": [{"key": "Exported count", "value": "50"}],
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
                    "userIdentity": {"principalId": "EXAMPLE"},
                    "requestParameters": {"sourceIPAddress": "127.0.0.1"},
                    "responseElements": {
                        "x-amz-request-id": "EXAMPLE123456789",
                        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
                    },
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "testConfigRule",
                        "bucket": {
                            "name": "results_verifier_test",
                            "ownerIdentity": {"principalId": "EXAMPLE"},
                            "arn": "arn:aws:s3:::example-bucket",
                        },
                        "object": {
                            "key": "results/query_results.json",
                            "size": 1024,
                            "eTag": "0123456789abcdef0123456789abcdef",
                            "sequencer": "0A1B2C3D4E5F678901",
                        },
                    },
                }
            ]
        }

    @staticmethod
    def get_record_containing_nulls():
        return {
            "query_results": [
                {
                    "query_details": {
                        "query_name": "Missing exported totals",
                    },
                    "query_results": [{"missing_exported_count": "null"}],
                },
                {
                    "query_details": {
                        "query_name": "Export totals",
                    },
                    "query_results": [{"exported_count": "null"}],
                },
            ]
        }

    @mock_s3
    def setUp_s3(self, json_record):
        s3_client = boto3.client("s3")
        s3_client.create_bucket(
            Bucket="results_verifier_test",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.put_object(
            Body=json.dumps(json_record),
            Bucket="results_verifier_test",
            Key="results/query_results.json",
        )
        event_handler.s3_client = s3_client

    @mock_sns
    def setup_sns(self, sqs_arn):
        sns_client = boto3.client(service_name="sns", region_name="eu-west-2")
        sns_client.create_topic(
            Name="monitoring_topic", Attributes={"DisplayName": "test-topic"}
        )
        topics_json = sns_client.list_topics()
        event_handler.sns_client = sns_client

        topic_arn = topics_json["Topics"][0]["TopicArn"]

        sns_client.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=sqs_arn)

        return topic_arn

    def setUp(self):
        event_handler.logger = logging.getLogger()
