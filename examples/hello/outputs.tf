output "bus_name" {
  value = module.bus.bus_name
}

output "event_source" {
  value = local.event_source
}

output "event_type" {
  value = local.event_type
}

# One entry per consumer, keyed `ledger` and `audit`.
output "consumers" {
  value = {
    for key in local.consumers : key => {
      function_name    = aws_lambda_function.hello[key].function_name
      ledger_table     = aws_dynamodb_table.ledger[key].name
      inbox_table      = module.consumer[key].inbox_table_name
      queue_url        = module.consumer[key].queue_url
      queue_arn        = module.consumer[key].queue_arn
      dlq_url          = module.consumer[key].dlq_url
      delivery_dlq_url = module.consumer[key].delivery_dlq_url
      mapping_uuid     = module.consumer[key].mapping_uuid
    }
  }
}

output "producer_policy_json" {
  description = "Attach to whatever role publishes `hello.said`: PutEvents on this bus with source example.hello only."
  value       = module.producer.policy_json
}
