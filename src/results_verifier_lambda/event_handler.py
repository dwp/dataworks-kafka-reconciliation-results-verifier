import argparse
import json
import logging
import os
import socket
import sys

import boto3
from botocore.config import Config

s3_client = None
sns_client = None

ERROR_NOTIFICATION_TYPE = "Error"
WARNING_NOTIFICATION_TYPE = "Warning"
INFORMATION_NOTIFICATION_TYPE = "Information"

CRITICAL_SEVERITY = "Critical"
HIGH_SEVERITY = "High"

args = None
logger = None


def handler(event, context):
    """Handle the event from AWS.

    Args:
        event (Object): The event details from AWS
        context (Object): The context info from AWS

    """
    global args
    global logger
    try:
        args = get_parameters()
        logger = setup_logging("INFO")
        logger.info(f'Cloudwatch Event": {event}')
        logger.info(os.getcwd())
        handle_event(event)
    except Exception as err:
        logger.error(f'Exception occurred for invocation", "error_message": {err}')


def get_parameters():
    """Parse the supplied command line arguments.

    Returns:
        args: The parsed and validated command line arguments

    """
    parser = argparse.ArgumentParser(
        description="Start up and shut down ASGs on demand"
    )

    # Parse command line inputs and set defaults
    parser.add_argument("--aws-profile", default="default")
    parser.add_argument("--aws-region", default="eu-west-2")
    parser.add_argument("--sns-topic", help="SNS topic ARN")
    parser.add_argument("--environment", help="Environment value", default="NOT_SET")
    parser.add_argument("--application", help="Application", default="NOT_SET")
    parser.add_argument("--log-level", help="Log level for lambda", default="INFO")

    _args = parser.parse_args()

    # Override arguments with environment variables where set
    if "AWS_PROFILE" in os.environ:
        _args.aws_profile = os.environ["AWS_PROFILE"]

    if "AWS_REGION" in os.environ:
        _args.aws_region = os.environ["AWS_REGION"]

    if "SNS_TOPIC" in os.environ:
        _args.sns_topic = os.environ["SNS_TOPIC"]

    if "ENVIRONMENT" in os.environ:
        _args.environment = os.environ["ENVIRONMENT"]

    if "APPLICATION" in os.environ:
        _args.application = os.environ["APPLICATION"]

    if "LOG_LEVEL" in os.environ:
        _args.log_level = os.environ["LOG_LEVEL"]

    return _args


# Initialise logging
def setup_logging(logger_level):
    """Set the default logger with json output."""
    the_logger = logging.getLogger()
    for old_handler in the_logger.handlers:
        the_logger.removeHandler(old_handler)

    new_handler = logging.StreamHandler(sys.stdout)
    hostname = socket.gethostname()

    json_format = (
        f'{{ "timestamp": "%(asctime)s", "log_level": "%(levelname)s", "message": "%(message)s", '
        f'"module": "%(module)s", "process":"%(process)s", '
        f'"thread": "[%(thread)s]", "host": "{hostname}" }}'
    )

    new_handler.setFormatter(logging.Formatter(json_format))
    the_logger.addHandler(new_handler)
    new_level = logging.getLevelName(logger_level)
    the_logger.setLevel(new_level)

    if the_logger.isEnabledFor(logging.DEBUG):
        # Log everything from boto3
        boto3.set_stream_logger()
        the_logger.debug(f'Using boto3", "version": "{boto3.__version__}')

    return the_logger


def handle_event(event):
    records = event.get("Records", [])
    for record in records:
        queries_json_record = get_query_results(record.get("s3"))
        missing_export_count, export_count = get_counts(queries_json_record)
        message_payload = generate_message_payload(missing_export_count, export_count)
        send_sns_message(message_payload, args.sns_topic)


def get_query_results(s3_event_object):
    bucket = s3_event_object["bucket"]["name"]
    key = s3_event_object["object"]["key"]
    results_file = get_s3_file(bucket, key)
    return results_file


def get_counts(queries_json_record):
    results_list = queries_json_record.get("query_results")
    filtered_list = [
        d
        for d in results_list
        if d["query_details"]["query_name"]
           in ["Missing exported totals", "Export totals"]
    ]
    missing_export_count = count_missing_exports(filtered_list)
    export_count = count_total_exports(filtered_list)
    return missing_export_count, export_count


def count_missing_exports(results_list):
    missing_exported_dict = [
        d
        for d in results_list
        if d["query_details"]["query_name"] == "Missing exported totals"
    ][0]
    logger.info(f"Missing exported id result object {missing_exported_dict}")
    count = count_query_results(missing_exported_dict, "missing_exported_count")
    logger.info(f"Missing exported count {count}")
    return count


def count_total_exports(results_list):
    total_exported_dict = [
        d for d in results_list if d["query_details"]["query_name"] == "Export totals"
    ][0]
    logger.info(f"Total exports result object {total_exported_dict}")
    count = count_query_results(total_exported_dict, "exported_count")
    logger.info(f"Exported count {count}")
    return count


def count_query_results(query_dict, result_name):
    query_results = query_dict["query_results"]
    count = 0
    for result in query_results:
        if result.get(result_name, 0) != "null":
            count += int(result.get(result_name, 0))

    return count


def generate_message_payload(missing_exported_count, exported_count):
    """Generates a payload for a monitoring message.

    Arguments:
        missing_exported_count (int): the count of the missing records
        exported_count (int): the count of the total exports

    """
    custom_elements = [{"key": "Exported count", "value": str(exported_count)}]

    if missing_exported_count == 0:
        severity = HIGH_SEVERITY
        notification_type = INFORMATION_NOTIFICATION_TYPE
        title_text = "Kafka reconciliation successful"
    else:
        severity = CRITICAL_SEVERITY
        notification_type = ERROR_NOTIFICATION_TYPE
        title_text = "Kafka reconciliation - missing records"
        custom_elements.append(
            {"key": "Missing exports count", "value": str(missing_exported_count)}
        )

    payload = {
        "severity": severity,
        "notification_type": notification_type,
        "slack_username": "AWS Lambda Notification",
        "title_text": title_text,
        "custom_elements": custom_elements,
    }

    dumped_payload = get_escaped_json_string(payload)
    logger.info(f'Generated monitoring SNS payload", "payload": {dumped_payload}')

    return payload


def get_escaped_json_string(json_string):
    try:
        escaped_string = json.dumps(json.dumps(json_string))
    except:
        escaped_string = json.dumps(json_string)

    return escaped_string


def get_client(service_name, region=None, read_timeout_seconds=120):
    """Creates a standardised boto3 client for the given service.

    Keyword arguments:
    service_name -- the aws service name (i.e. s3, lambda etc)
    region -- use a specific region for the client (defaults to no region with None)
    read_timeout -- timeout for operations, defaults to 120 seconds
    """
    logger.info(f"Getting client for service {service_name}")
    max_connections = 25 if service_name.lower() == "s3" else 10

    client_config = Config(
        read_timeout=read_timeout_seconds,
        max_pool_connections=max_connections,
        retries={"max_attempts": 10, "mode": "standard"},
    )

    if region is None:
        return boto3.client(service_name, config=client_config)
    else:
        return boto3.client(service_name, region_name=region, config=client_config)


def get_s3_file(bucket, key):
    global s3_client
    if s3_client is None:
        s3_client = get_client(service_name="s3")

    response = s3_client.get_object(Bucket=bucket, Key=key)
    logger.info(f"Response from S3 {response}")
    data = json.loads(response["Body"].read())
    return data


def send_sns_message(payload, sns_topic_arn):
    """Publishes the message to sns.

    Arguments:
        payload (dict): the payload to post to SNS
        sns_topic_arn (string): the arn for the SNS topic

    """
    global sns_client

    json_message = json.dumps(payload)

    dumped_payload = get_escaped_json_string(payload)
    logger.info(
        f'Publishing payload to SNS", "payload": {dumped_payload}, "sns_topic_arn": "{sns_topic_arn}"'
    )

    if sns_client is None:
        sns_client = get_client(service_name="sns")

    response = sns_client.publish(TopicArn=sns_topic_arn, Message=json_message)
    logger.info(f"Response from Sns {response}")
