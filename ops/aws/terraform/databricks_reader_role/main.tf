provider "aws" {
  region = var.aws_region
}

locals {
  external_id_enabled = trimspace(var.external_id) != ""
  current_prefix_base = trim(var.current_prefix, "/")
}

data "aws_iam_policy_document" "trust" {
  statement {
    sid    = "DatabricksUnityCatalogAssume"
    effect = "Allow"

    actions = [
      "sts:AssumeRole",
    ]

    principals {
      type = "AWS"

      identifiers = [
        var.databricks_unity_catalog_role_arn,
      ]
    }

    dynamic "condition" {
      for_each = local.external_id_enabled ? [1] : []
      content {
        test     = "StringEquals"
        variable = "sts:ExternalId"
        values   = [var.external_id]
      }
    }
  }

  statement {
    sid    = "SelfAssume"
    effect = "Allow"

    actions = [
      "sts:AssumeRole",
    ]

    principals {
      type = "AWS"

      identifiers = [
        "arn:aws:iam::${var.aws_account_id}:role/${var.role_name}",
      ]
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
      values = [
        local.current_prefix_base,
        "${local.current_prefix_base}/",
        "${local.current_prefix_base}/*",
      ]
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
