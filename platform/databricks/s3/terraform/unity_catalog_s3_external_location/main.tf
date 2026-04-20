provider "databricks" {
  config_file = pathexpand(var.databricks_config_file)
  profile     = var.databricks_profile
}

locals {
  external_location_read_only_principals = setsubtract(
    toset(var.external_location_read_files_principals),
    toset(var.external_location_create_external_table_principals),
  )
}

resource "databricks_storage_credential" "this" {
  name            = var.storage_credential_name
  comment         = var.storage_credential_comment
  read_only       = var.storage_credential_read_only
  skip_validation = var.skip_validation

  aws_iam_role {
    role_arn = var.aws_role_arn
  }
}

resource "databricks_external_location" "this" {
  name            = var.external_location_name
  url             = var.external_location_url
  credential_name = databricks_storage_credential.this.id
  comment         = var.external_location_comment
  read_only       = var.external_location_read_only
  skip_validation = var.skip_validation
}

resource "databricks_grants" "storage_credential_create_external_location" {
  count = length(var.storage_credential_create_external_location_principals) == 0 ? 0 : 1

  storage_credential = databricks_storage_credential.this.id

  dynamic "grant" {
    for_each = toset(var.storage_credential_create_external_location_principals)

    content {
      principal  = grant.value
      privileges = ["CREATE_EXTERNAL_LOCATION"]
    }
  }
}

resource "databricks_grants" "external_location" {
  count = (
    length(local.external_location_read_only_principals) == 0 &&
    length(var.external_location_create_external_table_principals) == 0
  ) ? 0 : 1

  external_location = databricks_external_location.this.id

  dynamic "grant" {
    for_each = local.external_location_read_only_principals

    content {
      principal  = grant.value
      privileges = ["READ_FILES"]
    }
  }

  dynamic "grant" {
    for_each = toset(var.external_location_create_external_table_principals)

    content {
      principal  = grant.value
      privileges = ["CREATE_EXTERNAL_TABLE", "READ_FILES"]
    }
  }
}
