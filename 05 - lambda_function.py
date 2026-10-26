"""Consume filtered Kinesis accident events and publish metrics and alerts."""

import base64
import json
import logging
import os

import boto3


LOGGER = logging.getLogger()
LOGGER.setLevel(os.getenv("LOG_LEVEL", "INFO"))

CLOUDWATCH_NAMESPACE = os.environ["CLOUDWATCH_NAMESPACE"]
CLOUDWATCH_METRIC = os.environ["CLOUDWATCH_METRIC"]
TOPIC_ARN = os.environ["TOPIC_ARN"]
ALERT_SEVERITY_THRESHOLD = int(os.getenv("ALERT_SEVERITY_THRESHOLD", "5"))

cloudwatch = boto3.client("cloudwatch")
sns = boto3.client("sns")


def decode_record(record):
    payload = base64.b64decode(record["kinesis"]["data"])
    document = json.loads(payload.decode("utf-8"))
    required = ("ID", "Severity", "City", "County")
    missing = [field for field in required if document.get(field) in (None, "")]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")
    document["Severity"] = int(document["Severity"])
    return document


def process_record(record):
    document = decode_record(record)
    severity = document["Severity"]
    cloudwatch.put_metric_data(
        Namespace=CLOUDWATCH_NAMESPACE,
        MetricData=[{
            "MetricName": CLOUDWATCH_METRIC,
            "Dimensions": [
                {"Name": "severity_accident", "Value": str(severity)},
                {"Name": "city_accident", "Value": str(document["City"])},
                {"Name": "county_accident", "Value": str(document["County"])},
            ],
            "Unit": "Count",
            "Value": 1,
            "StorageResolution": 1,
        }],
    )

    if severity >= ALERT_SEVERITY_THRESHOLD:
        sns.publish(
            TopicArn=TOPIC_ARN,
            Message=json.dumps(document, separators=(",", ":")),
            Subject=f"Accident alert: severity {severity}",
        )
    LOGGER.info("Processed accident id=%s severity=%s", document["ID"], severity)


def lambda_handler(event, context):
    failures = []
    for record in event.get("Records", []):
        sequence_number = record["kinesis"]["sequenceNumber"]
        try:
            process_record(record)
        except Exception:
            LOGGER.exception("Failed to process Kinesis record %s", sequence_number)
            failures.append({"itemIdentifier": sequence_number})
    return {"batchItemFailures": failures}
