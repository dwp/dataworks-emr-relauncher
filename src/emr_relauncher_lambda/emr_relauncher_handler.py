#!/usr/bin/env python3

"""emr_relauncher_lambda"""
import argparse
import boto3
import json
import logging
import os
import sys
import socket
import re
import datetime

args = None
logger = None


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
        f'"environment": "{args.environment}", "application": "{args.application}", '
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


def get_parameters():
    """Parse the supplied command line arguments.

    Returns:
        args: The parsed and validated command line arguments

    """
    parser = argparse.ArgumentParser()

    # Parse command line inputs and set defaults
    parser.add_argument("--aws-profile", default="default")
    parser.add_argument("--aws-region", default="eu-west-2")
    parser.add_argument("--sns-topic", help="SNS topic ARN")
    parser.add_argument("--environment", help="Environment value", default="NOT_SET")
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

    if "LOG_LEVEL" in os.environ:
        _args.log_level = os.environ["LOG_LEVEL"]

    return _args


def get_sns_client():
    return boto3.client("sns")


def handler(event, context):
    """Handle the event from AWS.

    Args:
        event (Object): The event details from AWS
        context (Object): The context info from AWS

    """
    global args
    global logger

    args = get_parameters()
    logger = setup_logging(args.log_level)

    dumped_event = get_escaped_json_string(event)
    logger.info(f'Cloudwatch Event", "sns_event": {dumped_event}, "mode": "handler')

    cluster_id = get_cluster_id(dumped_event)

    correlation_id, sns_prefix = query_dynamo(cluster_id)

    sns_client = get_sns_client()

    if not args.sns_topic:
        raise Exception("Required argument SNS_TOPIC is unset")

    payload = generate_lambda_launcher_payload(
        correlation_id, sns_prefix

    )

    send_sns_message(
        sns_client,
        payload,
        args.sns_topic
    )


def send_sns_message(
        sns_client,
        payload,
        sns_topic_arn
):
    """Publishes the message to sns.

    Arguments:
        sns_client (client): The boto3 client for SQS
        payload (dict): the payload to post to SNS
        sns_topic_arn (string): the arn for the SNS topice

    """
    global logger

    json_message = json.dumps(payload)

    dumped_payload = get_escaped_json_string(payload)
    logger.info(
        f'Publishing payload to SNS", "payload": {dumped_payload}, "sns_topic_arn": "{sns_topic_arn}"'
    )

    return sns_client.publish(TopicArn=sns_topic_arn, Message=json_message)


def get_escaped_json_string(json_string):
    try:
        escaped_string = json.dumps(json.dumps(json_string))
    except:
        escaped_string = json.dumps(json_string)

    return escaped_string


def get_cluster_id(json_event):
    pass


def query_dynamo(cluster_id):
    pass


def generate_lambda_launcher_payload(correlation_id, sns_prefix):
    pass


if __name__ == "__main__":
    try:
        args = get_parameters()
        logger = setup_logging("INFO")

        boto3.setup_default_session(
            profile_name=args.aws_profile, region_name=args.aws_region
        )
        logger.info(os.getcwd())
        json_content = json.loads(open("resources/event.json", "r").read())
        handler(json_content, None)
    except Exception as err:
        logger.error(f'Exception occurred for invocation", "error_message": {err}')
