import logging
import os
import socket
import sys

import boto3

from src.results_verifier_lambda.utility import aws_helper

logger = None


def handler(event, context):
    """Handle the event from AWS.

    Args:
        event (Object): The event details from AWS
        context (Object): The context info from AWS

    """
    global logger
    logger = setup_logging("INFO")
    logger.info(f'Cloudwatch Event": {event}')
    try:
        logger.info(os.getcwd())
        handle_event(event)
    except Exception as err:
        logger.error(f'Exception occurred for invocation", "error_message": {err}')


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
    records = event.get('Records', [])
    for record in records:
        queries_json_record = get_query_results(record.get('s3'))
        missing_export_count = count_missing_exports(queries_json_record)


def get_query_results(s3_event_object):
    bucket = s3_event_object['bucket']['name']
    key = s3_event_object['object']['key']
    results_file = aws_helper.get_s3_file(bucket, key)
    return results_file


def count_missing_exports(queries_json_record):
    results_list = queries_json_record.get("query_results")
    query_results = filter(lambda d: d['query_details']['query_name'] == "Missing exported ids", results_list)
    logger.info(f'Missing exported id result object {query_results}')
    return query_results
