variable "aws_region" {
  description = "AWS region for the bucket."
  type        = string
}

variable "bucket_name" {
  description = "Globally unique S3 bucket name."
  type        = string
}

variable "tags" {
  description = "Additional bucket tags."
  type        = map(string)
  default     = {}
}
