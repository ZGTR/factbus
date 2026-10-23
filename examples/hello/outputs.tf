output "bus_name" {
  value = module.bus.bus_name
}

output "event_source" {
  value = local.event_source
}

output "event_type" {
  value = local.event_type
}

output "function_name" {
  value = aws_lambda_function.hello.function_name
}

output "ledger_table" {
  value = aws_dynamodb_table.ledger.name
}

output "inbox_table" {
  value = module.consumer.inbox_table_name
}

output "queue_url" {
  value = module.consumer.queue_url
}

output "queue_arn" {
  value = module.consumer.queue_arn
}

output "dlq_url" {
  value = module.consumer.dlq_url
}

output "mapping_uuid" {
  description = "The `<id>` for docs/kill-switch-runbook.md."
  value       = module.consumer.mapping_uuid
}

output "producer_policy_json" {
  description = "Attach to whatever role publishes `hello.said`: PutEvents on this bus with source example.hello only."
  value       = module.producer.policy_json
}
