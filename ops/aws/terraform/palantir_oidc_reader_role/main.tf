provider "aws" {
  region = var.aws_region
}

locals {
  oidc_hostpath = replace(var.oidc_issuer_url, "https://", "")
}

data "aws_iam_policy_document" "trust" {
  statement {
    sid    = "PalantirOidcAssume"
    effect = "Allow"

    actions = [
      "sts:AssumeRoleWithWebIdentity",
    ]

    principals {
      type = "Federated"

      identifiers = [
        var.oidc_provider_arn,
      ]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_hostpath}:aud"
      values   = [var.oidc_audience]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_hostpath}:sub"
      values   = [var.source_rid]
    }
  }
}

data "aws_iam_policy_document" "read_only" {
  statement {
    sid    = "ListCurrentPrefix"
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]

    resources = [
      "arn:aws:s3:::${var.bucket_name}",
    ]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${trim(var.current_prefix, "/")}/*"]
    }
  }

  statement {
    sid    = "ReadCurrentObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject",
    ]

    resources = [
      "arn:aws:s3:::${var.bucket_name}/${trim(var.current_prefix, "/")}/*",
    ]
  }
}

resource "aws_iam_role" "reader" {
  name               = var.role_name
  assume_role_policy = data.aws_iam_policy_document.trust.json
}

resource "aws_iam_role_policy" "reader" {
  name   = "${var.role_name}-policy"
  role   = aws_iam_role.reader.id
  policy = data.aws_iam_policy_document.read_only.json
}
