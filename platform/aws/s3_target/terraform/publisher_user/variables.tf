variable "aws_region" {
  description = "AWS region for provider operations."
  type        = string
}

variable "bucket_name" {
  description = "Bucket the publisher can manage."
  type        = string
}

variable "allowed_prefixes" {
  description = "Optional allowed S3 prefixes for publisher access, for example [\"prod/*\"]. Empty means the whole bucket."
  type        = list(string)
  default     = []
}

variable "group_name" {
  description = "IAM group name."
  type        = string
}

variable "policy_name" {
  description = "IAM policy name."
  type        = string
}

variable "user_name" {
  description = "IAM user name."
  type        = string
}
