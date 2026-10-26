data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "consumer" {
  name               = "${local.name_prefix}-consumer-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "consumer" {
  statement {
    sid = "ReadFilteredStream"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards",
      "kinesis:SubscribeToShard"
    ]
    resources = [aws_kinesis_stream.filtered.arn]
  }

  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.consumer.arn}:*"]
  }

  statement {
    sid       = "PublishMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["RealtimeAccidentAnalysis/${var.environment}"]
    }
  }

  statement {
    sid       = "PublishCriticalAlerts"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.critical_alerts.arn]
  }

  statement {
    sid       = "SendFailedRecords"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.consumer_failures.arn]
  }
}

resource "aws_iam_role_policy" "consumer" {
  name   = "${local.name_prefix}-consumer-policy"
  role   = aws_iam_role.consumer.id
  policy = data.aws_iam_policy_document.consumer.json
}

data "aws_iam_policy_document" "flink_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["kinesisanalytics.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "flink" {
  name               = "${local.name_prefix}-flink-role"
  assume_role_policy = data.aws_iam_policy_document.flink_assume_role.json
}

data "aws_iam_policy_document" "flink" {
  statement {
    sid = "ReadInputStream"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards"
    ]
    resources = [aws_kinesis_stream.input.arn]
  }

  statement {
    sid = "WriteFilteredStream"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards",
      "kinesis:PutRecord",
      "kinesis:PutRecords",
      "kinesis:SubscribeToShard"
    ]
    resources = [aws_kinesis_stream.filtered.arn]
  }

  statement {
    sid = "UseStudioCatalog"
    actions = [
      "glue:CreateTable",
      "glue:GetConnection",
      "glue:GetDatabase",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:GetTable",
      "glue:GetTables",
      "glue:UpdateTable"
    ]
    resources = [
      aws_glue_catalog_database.flink.arn,
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.flink.name}/*",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:connection/*"
    ]
  }

  statement {
    sid       = "ListStudioCatalogDatabases"
    actions   = ["glue:GetDatabases"]
    resources = ["*"]
  }

  statement {
    sid       = "ListStudioLogs"
    actions   = ["logs:DescribeLogGroups", "logs:DescribeLogStreams", "logs:FilterLogEvents", "logs:GetLogEvents"]
    resources = ["*"]
  }

  statement {
    sid       = "ListStudioArtifacts"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.source.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["studio-artifacts/*"]
    }
  }

  statement {
    sid       = "ManageStudioArtifacts"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.source.arn}/studio-artifacts/*"]
  }
}

resource "aws_iam_role_policy" "flink" {
  name   = "${local.name_prefix}-flink-policy"
  role   = aws_iam_role.flink.id
  policy = data.aws_iam_policy_document.flink.json
}

data "aws_iam_policy_document" "producer" {
  statement {
    sid       = "ReadSampleData"
    actions   = ["s3:GetObject"]
    resources = [aws_s3_object.sample_data.arn]
  }

  statement {
    sid       = "WriteInputStream"
    actions   = ["kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:PutRecord", "kinesis:PutRecords"]
    resources = [aws_kinesis_stream.input.arn]
  }

}

resource "aws_iam_policy" "producer" {
  name        = "${local.name_prefix}-producer"
  description = "Permissions required by the local accident replay producer."
  policy      = data.aws_iam_policy_document.producer.json
}
