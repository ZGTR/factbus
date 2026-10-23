variable "name" {
  description = "Base name for the rule, the queue (`<name>`), its DLQ (`<name>-dlq`), the alarms and, by default, the inbox table (`<name>-inbox`)."
  type        = string
  validation {
    # SQS allows 80 characters; `-dlq` and `-dlq-visible` need room.
    condition     = can(regex("^[A-Za-z0-9_-]{1,64}$", var.name))
    error_message = "name: 1 to 64 letters, digits, '-' or '_' (it names an SQS queue and an EventBridge rule)."
  }
}

variable "bus_name" {
  description = "The EventBridge bus the producer publishes to (the `bus` module's `bus_name` output)."
  type        = string
  validation {
    condition     = length(var.bus_name) > 0
    error_message = "bus_name must not be empty: an empty name subscribes to the account's default bus."
  }
}

variable "sources" {
  description = "CloudEvents `source` values this consumer accepts, matched against EventBridge `source`."
  type        = list(string)
  validation {
    condition     = length(var.sources) > 0
    error_message = "sources: at least one; an empty list would match nothing."
  }
}

variable "event_types" {
  description = "CloudEvents `type` values this consumer accepts, matched against EventBridge `detail-type`."
  type        = list(string)
  validation {
    condition     = length(var.event_types) > 0
    error_message = "event_types: at least one; an empty list would match nothing."
  }
}

variable "function_arn" {
  description = "The Lambda function that drains the queue (ARN or name)."
  type        = string
}

variable "enabled" {
  description = <<-EOT
    The kill switch: the event source mapping's Enabled flag. False stops new receives;
    messages wait on the queue (docs/kill-switch-runbook.md). Set it here, not only with
    the CLI, so the next apply does not turn a thrown switch back on.
  EOT
  type        = bool
  default     = true
}

variable "batch_size" {
  description = "Messages per Lambda invocation."
  type        = number
  default     = 10
}

variable "maximum_batching_window_in_seconds" {
  description = "Seconds the poller may wait to fill a batch. 0 invokes as soon as a message arrives."
  type        = number
  default     = 0
}

variable "max_receive_count" {
  description = "Receives before a message moves to the DLQ."
  type        = number
  default     = 10
}

variable "function_timeout_seconds" {
  description = "The function's timeout. Queue visibility is six times this, as AWS recommends for Lambda sources."
  type        = number
  default     = 30
}

variable "retention_seconds" {
  description = "Queue and DLQ retention. 14 days, the SQS maximum."
  type        = number
  default     = 1209600
}

variable "age_alarm_threshold_seconds" {
  description = "ApproximateAgeOfOldestMessage threshold for the age alarm."
  type        = number
  default     = 1800
}

variable "alarm_topic_arn" {
  description = "SNS topic every alarm notifies. Null: the alarms exist but notify nobody."
  type        = string
  default     = null
}

variable "create_inbox_table" {
  description = "Create the DynamoDB inbox table (pk/sk strings, TTL on `expires_at`, PITR on, on-demand)."
  type        = bool
  default     = true
}

variable "inbox_table_name" {
  description = "Inbox table name. Null: `<name>-inbox`."
  type        = string
  default     = null
}

variable "role_name" {
  description = "The consumer function's IAM role. Set: `consumer_policy_json` is attached to it as an inline policy. Must be known at plan."
  type        = string
  default     = null
}
