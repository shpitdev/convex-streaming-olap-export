variable "aws_region" {
  type = string
}

variable "aws_account_id" {
  type = string
}

variable "bucket_name" {
  type = string
}

variable "current_prefix" {
  description = "Read-only current table prefix, for example prod/staging/current."
  type        = string
}

variable "role_name" {
  type = string
}

variable "databricks_unity_catalog_role_arn" {
  description = "Databricks-provided Unity Catalog role ARN."
  type        = string
}

variable "external_id" {
  description = "Databricks-provided external ID for the storage credential. Leave empty only for the initial bootstrap apply."
  type        = string
  default     = ""
}
