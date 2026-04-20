variable "databricks_config_file" {
  description = "Path to the Databricks CLI config file."
  type        = string
  default     = "~/.databrickscfg"
}

variable "databricks_profile" {
  description = "Databricks CLI profile to use."
  type        = string
  default     = "DEFAULT"
}

variable "aws_role_arn" {
  description = "AWS IAM role ARN that Databricks will use for S3 access."
  type        = string
}

variable "storage_credential_name" {
  description = "Unity Catalog storage credential name."
  type        = string
}

variable "storage_credential_comment" {
  description = "Optional storage credential comment."
  type        = string
  default     = "Managed by Terraform"
}

variable "storage_credential_read_only" {
  description = "Whether the storage credential is read-only."
  type        = bool
  default     = true
}

variable "external_location_name" {
  description = "Unity Catalog external location name."
  type        = string
}

variable "external_location_url" {
  description = "External location URL, for example s3://bucket/prod/staging/current/."
  type        = string
}

variable "external_location_comment" {
  description = "Optional external location comment."
  type        = string
  default     = "Managed by Terraform"
}

variable "external_location_read_only" {
  description = "Whether the external location is read-only."
  type        = bool
  default     = true
}

variable "skip_validation" {
  description = "Bootstrap mode for the AWS trust-policy chicken-and-egg."
  type        = bool
  default     = true
}

variable "storage_credential_create_external_location_principals" {
  description = "Principals to grant CREATE_EXTERNAL_LOCATION on the storage credential."
  type        = list(string)
  default     = []
}

variable "external_location_read_files_principals" {
  description = "Principals to grant READ_FILES on the external location."
  type        = list(string)
  default     = []
}

variable "external_location_create_external_table_principals" {
  description = "Principals to grant CREATE_EXTERNAL_TABLE and READ_FILES on the external location."
  type        = list(string)
  default     = []
}
