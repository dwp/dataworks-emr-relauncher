# DO NOT USE THIS REPO - MIGRATED TO GITLAB

# dataworks-emr-relauncher

## About the project

This application contains Python code that is responsible for relaunching an EMR cluster in the event of failure. The 
code is intended to be deployed as an AWS Lambda and triggered by a Cloudwatch event which contains a cluster_id 
within the payload.


A DynamoDb table will be queried for additional data about the cluster:
* Run_Id: How many times this cluster has ran.
* S3_Prefix: S3 path.
* CurrentStep: Which step failed. 

A cluster will be retried if the Run_Id is less than or equal to max retry setting, and CurrentStep is contained in the list of steps to retry. 

If the retry logic is satisfied a message will be constructed and sent to the provided SNS topic. 

Example message:
```json
{
    "correlation_id": "example_correlation_id",
    "s3_prefix": "path/path"
}
```

## Environment variables

|Variable name|Example|Description|Required|
|:---|:---|:---|:---|
|AWS_PROFILE| default |The profile for making AWS calls to other services |No|
|AWS_REGION| eu-west-1 |The region the lambda is running in |No|
|LOG_LEVEL| INFO |The logging level of the Lambda |No|
|SNS_TOPIC_ARN| arn:aws:sns:{region}:{account}:mytopic |The arn of the sns topic to send restart messages to|Yes|
|TABLE_NAME| Retry |The name of the DynamoDb table to query for Emr data|Yes|
|STEPS_NOT_TO_RETRY| collect-metrics,flush-pushgateway |Comma delimited list of steps that would not warrant a retry if they failed (defaults to none)|No|
|MAX_RETRY_COUNT| 2 |The maximum tries to retry the cluster (defaults to 1)|No|


## Local Setup

A Makefile is provided for project setup.

Run `make setup-local` This will create a Python virtual environment and install and dependencies. 

## Testing

There are tox unit tests in the module. To run them, you will need the module tox installed with pip install tox, then go to the root of the module and simply run tox to run all the unit tests.

The test may also be ran via `make unittest`.

You should always ensure they work before making a pull request for your branch.

If tox has an issue with Python version you have installed, you can specify such as `tox -e py38`.
