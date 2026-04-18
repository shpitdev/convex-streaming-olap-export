variable "aws_region" {
  type = string
}

variable "bucket_name" {
  type = string
}

variable "current_prefix" {
  type = string
}

variable "role_name" {
  type = string
}

variable "palantir_cloud_identity_role_arn" {
  description = "Foundry Control Panel Cloud Identity role ARN."
  type        = string
}
