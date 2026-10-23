variable "bus_arn" {
  description = "The EventBridge bus this producer publishes to (the `bus` module's `bus_arn` output)."
  type        = string
}

variable "event_source" {
  description = "The CloudEvents `source` this producer publishes as. PutEvents is allowed only with this source."
  type        = string
  validation {
    condition     = length(var.event_source) > 0
    error_message = "event_source must not be empty."
  }
}

variable "role_name" {
  description = "The producer's IAM role. Set: the policy is attached to it inline. Null: use the `policy_json` output. Must be known at plan."
  type        = string
  default     = null
}

variable "sweeper_function_arn" {
  description = "The outbox sweeper Lambda. Set: a schedule invokes it. Null: no schedule, no permission. Must be known at plan (pass an existing function's ARN)."
  type        = string
  default     = null
}

variable "sweeper_schedule_expression" {
  description = "How often the sweeper runs."
  type        = string
  default     = "rate(1 minute)"
}

variable "name" {
  description = "Base name for the schedule rule and the inline policy. Null: `<source>-sweeper`, with characters a rule name refuses replaced by '-'."
  type        = string
  default     = null
}
