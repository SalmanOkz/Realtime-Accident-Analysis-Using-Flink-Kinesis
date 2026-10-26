resource "aws_cloudwatch_metric_alarm" "consumer_errors" {
  alarm_name          = "${local.name_prefix}-consumer-errors"
  alarm_description   = "Consumer Lambda returned one or more invocation errors."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.consumer.function_name }
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.critical_alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "consumer_iterator_age" {
  alarm_name          = "${local.name_prefix}-consumer-iterator-age"
  alarm_description   = "Filtered-event consumer is more than one minute behind the stream."
  namespace           = "AWS/Lambda"
  metric_name         = "IteratorAge"
  dimensions          = { FunctionName = aws_lambda_function.consumer.function_name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 2
  threshold           = 60000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.critical_alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "failure_queue" {
  alarm_name          = "${local.name_prefix}-failure-queue-not-empty"
  alarm_description   = "One or more filtered Kinesis records exhausted Lambda retries."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.consumer_failures.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.critical_alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "firehose_delivery_freshness" {
  alarm_name          = "${local.name_prefix}-firehose-delivery-freshness"
  alarm_description   = "Firehose has not delivered enriched records to the curated S3 layer within five minutes."
  namespace           = "AWS/Firehose"
  metric_name         = "DeliveryToS3.DataFreshness"
  dimensions          = { DeliveryStreamName = aws_kinesis_firehose_delivery_stream.curated.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 2
  threshold           = 300
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.critical_alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "firehose_delivery_failure" {
  alarm_name          = "${local.name_prefix}-firehose-delivery-failure"
  alarm_description   = "Firehose received enriched records but failed to deliver them to S3."
  namespace           = "AWS/Firehose"
  metric_name         = "DeliveryToS3.Success"
  dimensions          = { DeliveryStreamName = aws_kinesis_firehose_delivery_stream.curated.name }
  statistic           = "Minimum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.critical_alerts.arn]
}

resource "aws_cloudwatch_dashboard" "pipeline" {
  dashboard_name = "${local.name_prefix}-pipeline"
  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Kinesis incoming records"
          region  = var.aws_region
          view    = "timeSeries"
          stacked = false
          metrics = [
            ["AWS/Kinesis", "IncomingRecords", "StreamName", aws_kinesis_stream.input.name],
            [".", ".", ".", aws_kinesis_stream.filtered.name]
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title  = "Firehose delivery"
          region = var.aws_region
          view   = "timeSeries"
          metrics = [
            ["AWS/Firehose", "DeliveryToS3.Records", "DeliveryStreamName", aws_kinesis_firehose_delivery_stream.curated.name],
            [".", "DeliveryToS3.DataFreshness", ".", "."]
          ]
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "Lambda consumer"
          region = var.aws_region
          view   = "timeSeries"
          metrics = [
            ["AWS/Lambda", "Invocations", "FunctionName", aws_lambda_function.consumer.function_name],
            [".", "Errors", ".", "."],
            [".", "IteratorAge", ".", "."]
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "Filtered accidents by severity"
          region = var.aws_region
          view   = "timeSeries"
          metrics = [
            ["RealtimeAccidentAnalysis/${var.environment}", "FilteredAccidentCount", "severity_accident", "4"],
            [".", ".", ".", "5"]
          ]
        }
      }
    ]
  })
}
