output "group_name" {
  value = aws_iam_group.publishers.name
}

output "policy_arn" {
  value = aws_iam_policy.publisher.arn
}

output "user_name" {
  value = aws_iam_user.publisher.name
}

output "access_key_id" {
  value = aws_iam_access_key.publisher.id
}

output "secret_access_key" {
  value     = aws_iam_access_key.publisher.secret
  sensitive = true
}
