#!/usr/bin/env python3

"""emr_relauncher_lambda"""
import argparse
import json
import logging
import os
import socket
import sys

import boto3
from boto3.dynamodb.conditions import Attr

args = None
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


def handle_event(event):
    global args

    args = get_environment_variables()

    if not args.sns_topic:
        raise Exception("Required environment variable SNS_TOPIC is unset")

    if not args.table_name:
        raise Exception("Required environment variable TABLE_NAME is unset")

    sns_client = get_sns_client()

    dynamo_client = get_dynamo_table(args.table_name)

    cluster_id = get_cluster_id(event)

    dynamo_items = query_dynamo(dynamo_client, cluster_id)

    if not dynamo_items:
        logger.info(
            f"No item found in DynamoDb table {args.table_name} for cluster_id {cluster_id}"
        )
    else:
        failed_item = dynamo_items[0]  # can only be one item
        failed_step = failed_item["CurrentStep"]

        run_id = failed_item["Run_Id"]

        if failed_step not in args.steps_not_to_retry and int(run_id) <= int(
            args.max_retry_count
        ):
            logger.info(f"Previous failed step was, {failed_step}. Relaunching cluster")

            payload = generate_lambda_launcher_payload(failed_item)

            sns_response = send_sns_message(sns_client, payload, args.sns_topic)
            logger.info(f"Response from Sns: {sns_response}.")
        else:
            logger.info(
                f"Previous failed step was, {failed_step}. Cluster not relaunching"
            )


def get_cluster_id(json_event):
    """Retrieves the cluster_id from the event

    Arguments:
        json_event (dict): The dict representing a Cloudwatch Event

    Returns:
        cluster_id: The cluster_id found in the event

    """
    cluster_id = json_event["detail"]["clusterId"]
    logger.info(f"Cluster Id {cluster_id}")
    return cluster_id


def query_dynamo(dynamo_table, cluster_id):
    """Queries the DynamoDb table

    Arguments:
        dynamo_table (table): The boto3 table for DynamoDb
        cluster_id (str): the cluster_id to query for

    Returns:
        list: The items matching the scan operation

    """
    response = dynamo_table.scan(FilterExpression=Attr("Cluster_Id").eq(cluster_id))
    logger.info(f"Response from dynamo {response}")
    return response["Items"]


def generate_lambda_launcher_payload(dynamo_item):
    data_products_using_s3_prefix_analytical = ["PDM", "CLIVE"]

    s3_prefix_value = (
        dynamo_item["S3_Prefix_Analytical_DataSet"]
        if dynamo_item["DataProduct"] in data_products_using_s3_prefix_analytical
        else dynamo_item["S3_Prefix_Snapshots"]
    )

    payload = {
        "correlation_id": dynamo_item["Correlation_Id"],
        "s3_prefix": s3_prefix_value,
        "snapshot_type": dynamo_item["Snapshot_Type"],
        "export_date": dynamo_item["Date"],
    }

    logger.info(f"Lambda payload: {payload}")
    return payload


def send_sns_message(sns_client, payload, sns_topic_arn):
    """Publishes the message to sns.

    Arguments:
        sns_client (client): The boto3 client for SQS
        payload (dict): the payload to post to SNS
        sns_topic_arn (string): the arn for the SNS topic

    """
    dumped_payload = get_escaped_json_string(payload)
    logger.info(
        f'Publishing payload to SNS", "payload": {dumped_payload}, "sns_topic_arn": "{sns_topic_arn}"'
    )

    return sns_client.publish(TopicArn=sns_topic_arn, Message=dumped_payload)


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


def get_environment_variables():
    """Retrieve the required environment variables.

    Returns:
        args: The parsed and validated environment variables

    """
    parser = argparse.ArgumentParser()

    _args = parser.parse_args()

    if "SNS_TOPIC" in os.environ:
        _args.sns_topic = os.environ["SNS_TOPIC"]

    if "TABLE_NAME" in os.environ:
        _args.table_name = os.environ["TABLE_NAME"]

    if "STEPS_TO_NOT_RETRY" in os.environ:
        _args.steps_not_to_retry = os.environ["STEPS_TO_NOT_RETRY"].split(",")
    else:
        _args.steps_not_to_retry = []

    _args.max_retry_count = os.environ.get("MAX_RETRY_COUNT", 1)

    if "LOG_LEVEL" in os.environ:
        _args.log_level = os.environ["LOG_LEVEL"]

    return _args


def get_sns_client():
    return boto3.client("sns")


def get_dynamo_table(table_name):
    """Retrieve the boto3 DynamoDb Table resource.

    Returns:
        table: A resource representing an Amazon DynamoDB Table

    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    return table


def get_escaped_json_string(json_string):
    escaped_string = json.dumps(json_string)
    logger.info(f"Escaped json: {json_string}")
    return escaped_string
