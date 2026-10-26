resource "aws_kinesis_stream" "input" {
  name             = "${local.name_prefix}-input"
  retention_period = 24
  encryption_type  = "KMS"
  kms_key_id       = "alias/aws/kinesis"
  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }
  tags = merge(local.common_tags, { DataStage = "input" })
}

resource "aws_kinesis_stream" "filtered" {
  name             = "${local.name_prefix}-enriched"
  retention_period = 24
  encryption_type  = "KMS"
  kms_key_id       = "alias/aws/kinesis"
  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }
  tags = merge(local.common_tags, { DataStage = "enriched" })
}
