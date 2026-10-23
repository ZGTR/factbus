output "policy_json" {
  description = "IAM policy allowing events:PutEvents on the bus with events:source = var.event_source, and events:DescribeEventBus."
  value       = local.policy_json
}

output "sweeper_rule_arn" {
  value = var.sweeper_function_arn == null ? null : aws_cloudwatch_event_rule.sweeper[0].arn
}

output "sweeper_rule_name" {
  value = var.sweeper_function_arn == null ? null : aws_cloudwatch_event_rule.sweeper[0].name
}
