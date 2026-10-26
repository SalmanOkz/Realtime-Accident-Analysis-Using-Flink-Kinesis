resource "aws_athena_workgroup" "analytics" {
  name        = "${local.name_prefix}-analytics"
  description = "Queries curated accident Parquet data."
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.analytics.id}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

resource "aws_athena_named_query" "severity_by_city" {
  name        = "${local.name_prefix}-severity-by-city"
  description = "Counts curated severe accidents by city and severity."
  database    = aws_glue_catalog_database.analytics.name
  workgroup   = aws_athena_workgroup.analytics.id
  query       = <<-SQL
    SELECT city, severity, count(*) AS accident_count
    FROM ${aws_glue_catalog_database.analytics.name}.${aws_glue_catalog_table.curated_accidents.name}
    GROUP BY city, severity
    ORDER BY accident_count DESC
    LIMIT 100
  SQL
}

resource "aws_athena_named_query" "hourly_accidents" {
  name        = "${local.name_prefix}-hourly-accidents"
  description = "Counts curated accidents by delivery partition hour."
  database    = aws_glue_catalog_database.analytics.name
  workgroup   = aws_athena_workgroup.analytics.id
  query       = <<-SQL
    SELECT year, month, day, hour, count(*) AS accident_count
    FROM ${aws_glue_catalog_database.analytics.name}.${aws_glue_catalog_table.curated_accidents.name}
    GROUP BY year, month, day, hour
    ORDER BY year DESC, month DESC, day DESC, hour DESC
  SQL
}
