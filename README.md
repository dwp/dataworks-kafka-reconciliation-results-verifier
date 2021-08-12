# dataworks-kafka-reconciliation-results-verifier

## About the project
This project code which is designed to be deployed within an Aws Lambda function, and triggered via an
S3 file upload. This file should contain query results producted by the kafka reconciliation job. 
This lambda will send alert messages based on the count of total and missing exports.  


|Variable name|Example|Description|Required|
|:---|:---|:---|:---|
|AWS_PROFILE| default |The profile for making AWS calls to other services|No|
|AWS_REGION| eu-west-1 |The region the lambda is running in|No|
|ENVIRONMENT| dev |The environment the lambda is running in|No|
|APPLICATION| batch-job-handler |The name of the application|No|
|LOG_LEVEL| INFO |The logging level of the Lambda|No|
|SNS_TOPIC| |The arn of the sns topic to send monitoring messages to|Yes|

## Local Setup

A Makefile is provided for project setup.

Run `make setup-local` This will create a Python virtual environment and install and dependencies. 

## Testing

There are tox unit tests in the module. To run them, you will need the module tox installed with pip install tox, then go to the root of the module and simply run tox to run all the unit tests.

The test may also be ran via `make unit-tests`.

You should always ensure they work before making a pull request for your branch.

If tox has an issue with Python version you have installed, you can specify such as `tox -e py38`.

