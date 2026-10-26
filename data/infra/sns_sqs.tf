resource "aws_sns_topic" "critical_alerts" {
  name = "${local.name_prefix}-critical-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count = var.alert_email == null ? 0 : 1

  topic_arn = aws_sns_topic.critical_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_sqs_queue" "consumer_failures" {
  name                      = "${local.name_prefix}-consumer-failures"
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
}
