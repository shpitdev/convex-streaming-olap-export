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

variable "oidc_provider_arn" {
  description = "AWS IAM OIDC provider ARN."
  type        = string
}

variable "oidc_issuer_url" {
  description = "Palantir OIDC issuer URL."
  type        = string
}

variable "oidc_audience" {
  description = "Configured Palantir audience for this source."
  type        = string
}

variable "source_rid" {
  description = "Palantir source RID used to scope trust."
  type        = string
}
