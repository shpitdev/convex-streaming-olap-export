provider "aws" {
  region = var.aws_region
}

locals {
  publisher_prefixes = length(var.allowed_prefixes) == 0 ? ["*"] : var.allowed_prefixes
}

data "aws_iam_policy_document" "publisher" {
  statement {
    sid    = "BucketRead"
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]

    resources = [
      "arn:aws:s3:::${var.bucket_name}",
    ]

    dynamic "condition" {
      for_each = length(var.allowed_prefixes) == 0 ? [] : [1]
      content {
        test     = "StringLike"
        variable = "s3:prefix"
        values   = local.publisher_prefixes
      }
    }
  }

  statement {
    sid    = "ObjectReadWrite"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
    ]

    resources = [
      for prefix in local.publisher_prefixes :
      prefix == "*" ?
      "arn:aws:s3:::${var.bucket_name}/*" :
      "arn:aws:s3:::${var.bucket_name}/${trimsuffix(prefix, "*")}*"
    ]
  }
}

resource "aws_iam_group" "publishers" {
  name = var.group_name
}

resource "aws_iam_policy" "publisher" {
  name   = var.policy_name
  policy = data.aws_iam_policy_document.publisher.json
}

resource "aws_iam_group_policy_attachment" "publisher" {
  group      = aws_iam_group.publishers.name
  policy_arn = aws_iam_policy.publisher.arn
}

resource "aws_iam_user" "publisher" {
  name = var.user_name
  path = "/"
}

resource "aws_iam_user_group_membership" "publisher" {
  user = aws_iam_user.publisher.name

  groups = [
    aws_iam_group.publishers.name,
  ]
}

resource "aws_iam_access_key" "publisher" {
  user = aws_iam_user.publisher.name
}
