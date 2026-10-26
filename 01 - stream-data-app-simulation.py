"""Replay accident records from S3 into an Amazon Kinesis data stream."""

import argparse
import csv
import io
import json
import os
import time
from datetime import datetime, timezone

import boto3
from dateutil import parser as date_parser


DEFAULT_OBJECT_KEY = "raw/US_Accidents_Dec20_updated_100samples.csv"
TIMESTAMP_FIELDS = ("Start_Time", "End_Time", "Weather_Timestamp")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=os.getenv("ACCIDENT_SOURCE_BUCKET"))
    parser.add_argument("--key", default=os.getenv("ACCIDENT_SOURCE_KEY", DEFAULT_OBJECT_KEY))
    parser.add_argument("--stream", default=os.getenv("KINESIS_INPUT_STREAM"))
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"))
    parser.add_argument("--delay-seconds", type=float, default=float(os.getenv("REPLAY_DELAY_SECONDS", "0.25")))
    parser.add_argument("--repeat", type=int, default=int(os.getenv("REPLAY_REPEAT", "1")))
    args = parser.parse_args()

    missing = [name for name, value in (("--bucket", args.bucket), ("--stream", args.stream)) if not value]
    if missing:
        parser.error(f"Missing required configuration: {', '.join(missing)}")
    if args.repeat < 1:
        parser.error("--repeat must be at least 1")
    if args.delay_seconds < 0:
        parser.error("--delay-seconds cannot be negative")
    return args


def normalize_record(row):
    record = dict(row)
    for field in TIMESTAMP_FIELDS:
        value = record.get(field)
        if value:
            record[field] = date_parser.parse(value).isoformat()

    record["Severity"] = int(record["Severity"])
    record["Txn_Timestamp"] = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    return record


def load_rows(s3_client, bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    with io.TextIOWrapper(response["Body"], encoding="utf-8-sig", newline="") as body:
        yield from csv.DictReader(body)


def replay_once(s3_client, kinesis_client, bucket, key, stream, delay_seconds):
    sent = 0
    failed = 0
    for row_number, row in enumerate(load_rows(s3_client, bucket, key), start=2):
        try:
            record = normalize_record(row)
            kinesis_client.put_record(
                StreamName=stream,
                Data=json.dumps(record, separators=(",", ":")).encode("utf-8"),
                PartitionKey=record["ID"],
            )
            sent += 1
            print(json.dumps({"status": "sent", "id": record["ID"], "severity": record["Severity"]}))
            if delay_seconds:
                time.sleep(delay_seconds)
        except Exception as exc:
            failed += 1
            print(json.dumps({"status": "failed", "row": row_number, "error": str(exc)}))
    return sent, failed


def main():
    args = parse_args()
    session = boto3.Session(region_name=args.region)
    s3_client = session.client("s3")
    kinesis_client = session.client("kinesis")

    total_sent = 0
    total_failed = 0
    for replay_number in range(1, args.repeat + 1):
        sent, failed = replay_once(
            s3_client, kinesis_client, args.bucket, args.key, args.stream, args.delay_seconds
        )
        total_sent += sent
        total_failed += failed
        print(json.dumps({"replay": replay_number, "sent": sent, "failed": failed}))

    if total_failed:
        raise SystemExit(f"Replay completed with failures: sent={total_sent}, failed={total_failed}")


if __name__ == "__main__":
    main()
