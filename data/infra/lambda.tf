data "archive_file" "consumer" {
  type        = "zip"
  output_path = "${path.module}/consumer.zip"
  source {
    content  = file("${path.module}/../../05 - lambda_function.py")
    filename = "lambda_function.py"
  }
}

resource "aws_cloudwatch_log_group" "consumer" {
  name              = "/aws/lambda/${local.name_prefix}-consumer"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "consumer" {
  function_name = "${local.name_prefix}-consumer"
  description   = "Creates accident metrics and critical SNS alerts from filtered events."
  role          = aws_iam_role.consumer.arn
  runtime       = "python3.13"
  handler       = "lambda_function.lambda_handler"
  architectures = ["arm64"]
  memory_size   = 256
  timeout       = 30

  filename         = data.archive_file.consumer.output_path
  source_code_hash = data.archive_file.consumer.output_base64sha256

  environment {
    variables = {
      CLOUDWATCH_NAMESPACE     = "RealtimeAccidentAnalysis/${var.environment}"
      CLOUDWATCH_METRIC        = "FilteredAccidentCount"
      TOPIC_ARN                = aws_sns_topic.critical_alerts.arn
      ALERT_SEVERITY_THRESHOLD = tostring(var.alert_severity_threshold)
      LOG_LEVEL                = "INFO"
    }
  }

  depends_on = [aws_cloudwatch_log_group.consumer, aws_iam_role_policy.consumer]
}

resource "aws_lambda_event_source_mapping" "filtered_consumer" {
  event_source_arn  = aws_kinesis_stream.filtered.arn
  function_name     = aws_lambda_function.consumer.arn
  starting_position = "LATEST"

  batch_size                         = 100
  maximum_batching_window_in_seconds = 2
  maximum_record_age_in_seconds      = 3600
  maximum_retry_attempts             = 5
  bisect_batch_on_function_error     = true
  function_response_types            = ["ReportBatchItemFailures"]

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.consumer_failures.arn
    }
  }
}
