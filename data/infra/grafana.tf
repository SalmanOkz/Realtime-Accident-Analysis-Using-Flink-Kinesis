resource "aws_grafana_workspace" "analytics" {
  count = var.enable_grafana ? 1 : 0

  name                      = "${local.name_prefix}-analytics"
  description               = "Accident analytics dashboards backed by Athena and CloudWatch."
  account_access_type       = "CURRENT_ACCOUNT"
  authentication_providers  = ["AWS_SSO"]
  permission_type           = "SERVICE_MANAGED"
  data_sources              = ["ATHENA", "CLOUDWATCH"]
  notification_destinations = ["SNS"]
}

resource "aws_grafana_role_association" "admins" {
  count = var.enable_grafana && length(var.grafana_admin_user_ids) > 0 ? 1 : 0

  role         = "ADMIN"
  user_ids     = var.grafana_admin_user_ids
  workspace_id = aws_grafana_workspace.analytics[0].id
}
