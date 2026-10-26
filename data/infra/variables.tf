variable "aws_region" {
  description = "AWS Region for every component in this pipeline."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Short project name used in resource names and tags."
  type        = string
  default     = "accident-analysis"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,24}$", var.project_name))
    error_message = "project_name must be 3-25 lowercase letters, digits, or hyphens and start with a letter."
  }
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "alert_email" {
  description = "Optional email address subscribed to severity alerts. Confirmation is required after apply."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.alert_email == null || can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.alert_email))
    error_message = "alert_email must be null or a valid email address."
  }
}

variable "source_object_key" {
  description = "S3 key used for the sample accident CSV."
  type        = string
  default     = "raw/US_Accidents_Dec20_updated_100samples.csv"
}

variable "alert_severity_threshold" {
  description = "Minimum severity that causes the consumer Lambda to publish an SNS alert."
  type        = number
  default     = 5

  validation {
    condition     = var.alert_severity_threshold >= 1 && var.alert_severity_threshold <= 5
    error_message = "alert_severity_threshold must be between 1 and 5."
  }
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention period."
  type        = number
  default     = 14
}

variable "firehose_buffer_size_mb" {
  description = "Firehose buffer size before delivering Parquet files to S3."
  type        = number
  default     = 64

  validation {
    condition     = var.firehose_buffer_size_mb >= 64 && var.firehose_buffer_size_mb <= 128
    error_message = "firehose_buffer_size_mb must be between 64 and 128 when Parquet conversion is enabled."
  }
}

variable "firehose_buffer_interval_seconds" {
  description = "Maximum Firehose buffering interval before S3 delivery."
  type        = number
  default     = 60

  validation {
    condition     = var.firehose_buffer_interval_seconds >= 60 && var.firehose_buffer_interval_seconds <= 900
    error_message = "firehose_buffer_interval_seconds must be between 60 and 900."
  }
}

variable "enable_grafana" {
  description = "Create an Amazon Managed Grafana workspace. AWS IAM Identity Center must already be enabled in this Region."
  type        = bool
  default     = false
}

variable "grafana_admin_user_ids" {
  description = "Optional IAM Identity Center user IDs to assign as Grafana workspace administrators."
  type        = set(string)
  default     = []
}
