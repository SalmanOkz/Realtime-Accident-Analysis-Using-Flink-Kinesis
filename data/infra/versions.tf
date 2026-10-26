terraform {
  required_version = ">= 1.6.0"

  required_providers {
    archive = { source = "hashicorp/archive", version = "~> 2.7" }
    aws     = { source = "hashicorp/aws", version = "~> 6.0" }
    local   = { source = "hashicorp/local", version = "~> 2.5" }
    random  = { source = "hashicorp/random", version = "~> 3.7" }
  }
}
