locals {
  name_prefix = "${var.project_name}-${var.environment}"
  common_tags = { Component = "realtime-accident-pipeline" }
  accident_columns = [
    { name = "id", type = "string" },
    { name = "severity", type = "bigint" },
    { name = "city", type = "string" },
    { name = "county", type = "string" },
    { name = "txn_timestamp", type = "string" }
  ]
}
