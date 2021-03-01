#!/usr/bin/env python3

"""emr_relauncher_lambda"""
import argparse
import unittest
from unittest import mock

import boto3
from moto import mock_dynamodb2

from emr_relauncher_lambda import event_handler

SNS_TOPIC_ARN = "test-sns-topic-arn"
TABLE_NAME = "data_pipeline_metadata"

args = argparse.Namespace()
args.sns_topic = SNS_TOPIC_ARN
args.table_name = TABLE_NAME
args.log_level = "INFO"
args.max_retry_count = 1
args.steps_not_to_retry = ""


class TestRelauncher(unittest.TestCase):
    @mock_dynamodb2
    @mock.patch("emr_relauncher_lambda.event_handler.logger")
    def test_query_dynamo_item_exists(self, mock_logger):
        dynamodb_resource = self.mock_get_dynamodb_table("transform")
        result = event_handler.query_dynamo(dynamodb_resource, "test_cluster_id")
        self.assertEqual(
            result,
            [
                {
                    "Correlation_Id": "test_correlation_id",
                    "DataProduct": "PDM",
                    "CurrentStep": "transform",
                    "S3_Prefix": "test_s3_prefix",
                    "Cluster_Id": "test_cluster_id",
                    "Run_Id": 1,
                }
            ],
        )

    @mock_dynamodb2
    @mock.patch("emr_relauncher_lambda.event_handler.logger")
    def test_query_dynamo_item_empty_result(self, mock_logger):
        dynamodb_resource = self.mock_get_dynamodb_table("transform")
        result = event_handler.query_dynamo(dynamodb_resource, "invalid_cluster_id")
        self.assertEqual(result, [])

    @mock.patch("emr_relauncher_lambda.event_handler.send_sns_message")
    @mock.patch("emr_relauncher_lambda.event_handler.setup_logging")
    @mock.patch("emr_relauncher_lambda.event_handler.get_environment_variables")
    @mock.patch("emr_relauncher_lambda.event_handler.get_dynamo_table")
    @mock.patch("emr_relauncher_lambda.event_handler.get_sns_client")
    @mock.patch("emr_relauncher_lambda.event_handler.logger")
    @mock_dynamodb2
    def test_handler_sns_message_sent(
        self,
        mock_logger,
        get_sns_client_mock,
        get_dynamo_table_mock,
        get_environment_variables_mock,
        setup_logging_mock,
        send_sns_message_mock,
    ):
        dynamodb_resource = self.mock_get_dynamodb_table("transform")
        get_dynamo_table_mock.return_value = dynamodb_resource

        sns_client_mock = mock.MagicMock()
        get_sns_client_mock.return_value = sns_client_mock
        get_environment_variables_mock.return_value = args

        event = self.get_example_event("test_cluster_id")

        event_handler.handler(event, None)

        send_sns_message_mock.assert_called_once_with(
            sns_client_mock,
            {"correlation_id": "test_correlation_id", "s3_prefix": "test_s3_prefix"},
            args.sns_topic,
        )

    @mock.patch("emr_relauncher_lambda.event_handler.send_sns_message")
    @mock.patch("emr_relauncher_lambda.event_handler.setup_logging")
    @mock.patch("emr_relauncher_lambda.event_handler.get_environment_variables")
    @mock.patch("emr_relauncher_lambda.event_handler.get_dynamo_table")
    @mock.patch("emr_relauncher_lambda.event_handler.get_sns_client")
    @mock.patch("emr_relauncher_lambda.event_handler.logger")
    @mock_dynamodb2
    def test_handler_failing_event_not_retried(
        self,
        mock_logger,
        get_sns_client_mock,
        get_dynamo_table_mock,
        get_environment_variables_mock,
        setup_logging_mock,
        send_sns_message_mock,
    ):
        args.steps_not_to_retry = "collect_metrics"
        dynamodb_resource = self.mock_get_dynamodb_table("collect_metrics")
        get_dynamo_table_mock.return_value = dynamodb_resource

        sns_client_mock = mock.MagicMock()
        get_sns_client_mock.return_value = sns_client_mock
        get_environment_variables_mock.return_value = args

        event = self.get_example_event("test_cluster_id")

        event_handler.handler(event, None)

        send_sns_message_mock.assert_not_called()
        args.steps_not_to_retry = ""

    @mock.patch("emr_relauncher_lambda.event_handler.send_sns_message")
    @mock.patch("emr_relauncher_lambda.event_handler.setup_logging")
    @mock.patch("emr_relauncher_lambda.event_handler.get_environment_variables")
    @mock.patch("emr_relauncher_lambda.event_handler.get_dynamo_table")
    @mock.patch("emr_relauncher_lambda.event_handler.get_sns_client")
    @mock.patch("emr_relauncher_lambda.event_handler.logger")
    @mock_dynamodb2
    def test_handler_max_retries_breached_not_retried(
        self,
        mock_logger,
        get_sns_client_mock,
        get_dynamo_table_mock,
        get_environment_variables_mock,
        setup_logging_mock,
        send_sns_message_mock,
    ):
        args.max_retry_count = 0
        dynamodb_resource = self.mock_get_dynamodb_table("transform")
        get_dynamo_table_mock.return_value = dynamodb_resource

        sns_client_mock = mock.MagicMock()
        get_sns_client_mock.return_value = sns_client_mock
        get_environment_variables_mock.return_value = args

        event = self.get_example_event("test_cluster_id")

        event_handler.handler(event, None)

        send_sns_message_mock.assert_not_called()
        args.max_retry_count = 1

    @mock.patch("emr_relauncher_lambda.event_handler.setup_logging")
    @mock.patch("emr_relauncher_lambda.event_handler.get_environment_variables")
    @mock.patch("emr_relauncher_lambda.event_handler.get_sns_client")
    @mock.patch("emr_relauncher_lambda.event_handler.logger")
    def test_handler_invalid_environment_variable(
        self,
        mock_logger,
        get_sns_client_mock,
        get_environment_variables_mock,
        setup_logging_mock,
    ):
        sns_client_mock = mock.MagicMock()
        get_sns_client_mock.return_value = sns_client_mock
        get_environment_variables_mock.return_value = argparse.Namespace()

        event = self.get_example_event("test_cluster_id")

        with self.assertRaises(Exception) as context:
            event_handler.handle_event(event)

    @mock_dynamodb2
    def mock_get_dynamodb_table(self, failed_step):
        dynamodb_client = boto3.client("dynamodb", region_name="eu-west-2")
        dynamodb_client.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "Correlation_Id", "KeyType": "HASH"},  # Partition key
                {"AttributeName": "DataProduct", "KeyType": "RANGE"},  # Sort key
            ],
            AttributeDefinitions=[
                {"AttributeName": "Correlation_Id", "AttributeType": "S"},
                {"AttributeName": "DataProduct", "AttributeType": "S"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )
        dynamodb = boto3.resource("dynamodb", region_name="eu-west-2")
        table = dynamodb.Table(TABLE_NAME)

        item = {
            "Correlation_Id": "test_correlation_id",
            "DataProduct": "PDM",
            "CurrentStep": failed_step,
            "S3_Prefix": "test_s3_prefix",
            "Cluster_Id": "test_cluster_id",
            "Run_Id": 1,
        }

        table.put_item(TableName=TABLE_NAME, Item=item)

        return table

    @staticmethod
    def get_example_event(cluster_id):
        return {
            "detail": {
                "severity": "CRITICAL",
                "stateChangeReason": '{"code":"STEP_FAILURE","message":"Shut down as step failed"}',
                "name": "pdm-dataset-generator",
                "clusterId": cluster_id,
                "state": "TERMINATED_WITH_ERRORS",
                "message": "Amazon EMR Cluster j-1A6KVTAXNFCW0 (pdm-dataset-generator) has terminated with errors",
            }
        }
