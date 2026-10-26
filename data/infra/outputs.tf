output "aws_region" {
  description = "AWS Region containing the pipeline."
  value       = var.aws_region
}

output "source_bucket_name" {
  description = "S3 bucket containing the sample CSV."
  value       = aws_s3_bucket.source.id
}

output "source_object_key" {
  description = "S3 key of the sample CSV."
  value       = aws_s3_object.sample_data.key
}

output "input_stream_name" {
  description = "Kinesis stream read by Flink."
  value       = aws_kinesis_stream.input.name
}

output "filtered_stream_name" {
  description = "Backward-compatible output for the enriched stream written by Flink."
  value       = aws_kinesis_stream.filtered.name
}

output "enriched_stream_name" {
  description = "Kinesis stream written by Flink and independently consumed by Lambda and Firehose."
  value       = aws_kinesis_stream.filtered.name
}

output "analytics_bucket_name" {
  description = "S3 bucket containing curated Parquet files and Athena query results."
  value       = aws_s3_bucket.analytics.id
}

output "firehose_delivery_stream_name" {
  description = "Firehose delivery stream reading enriched Flink output."
  value       = aws_kinesis_firehose_delivery_stream.curated.name
}

output "analytics_glue_database_name" {
  description = "Glue database containing enriched and curated accident table definitions."
  value       = aws_glue_catalog_database.analytics.name
}

output "curated_glue_table_name" {
  description = "Glue/Athena table over curated Parquet accident data."
  value       = aws_glue_catalog_table.curated_accidents.name
}

output "athena_workgroup_name" {
  description = "Athena workgroup configured for curated accident queries."
  value       = aws_athena_workgroup.analytics.name
}

output "cloudwatch_dashboard_name" {
  description = "CloudWatch operational dashboard for the complete pipeline."
  value       = aws_cloudwatch_dashboard.pipeline.dashboard_name
}

output "grafana_workspace_endpoint" {
  description = "Managed Grafana endpoint when enable_grafana is true."
  value       = try(aws_grafana_workspace.analytics[0].endpoint, null)
}

output "flink_execution_role_arn" {
  description = "IAM role to select when creating the manual Flink Studio application."
  value       = aws_iam_role.flink.arn
}

output "flink_glue_database_name" {
  description = "Glue database to select when creating the Flink Studio notebook."
  value       = aws_glue_catalog_database.flink.name
}

output "generated_flink_sql_file" {
  description = "Generated SQL to run in the Flink Studio notebook."
  value       = local_file.flink_sql.filename
}

output "consumer_lambda_name" {
  description = "Name of the filtered-event Lambda consumer."
  value       = aws_lambda_function.consumer.function_name
}

output "alert_topic_arn" {
  description = "SNS topic used for critical alerts and pipeline alarms."
  value       = aws_sns_topic.critical_alerts.arn
}

output "producer_policy_arn" {
  description = "Attach this policy to the IAM identity used to run the local producer."
  value       = aws_iam_policy.producer.arn
}

output "producer_powershell" {
  description = "PowerShell environment variables required by the producer."
  value = join("\n", [
    "$env:AWS_REGION='${var.aws_region}'",
    "$env:ACCIDENT_SOURCE_BUCKET='${aws_s3_bucket.source.id}'",
    "$env:ACCIDENT_SOURCE_KEY='${aws_s3_object.sample_data.key}'",
    "$env:KINESIS_INPUT_STREAM='${aws_kinesis_stream.input.name}'"
  ])
}
