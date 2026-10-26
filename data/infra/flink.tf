resource "local_file" "flink_sql" {
  filename = "${path.module}/generated-flink.sql"
  content = templatefile("${path.module}/templates/flink.sql.tftpl", {
    aws_region           = var.aws_region
    input_stream_name    = aws_kinesis_stream.input.name
    filtered_stream_name = aws_kinesis_stream.filtered.name
  })
}
