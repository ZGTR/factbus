variable "prefix" {
  description = "Prefix for every resource name, so several copies of the example (or a sandbox run beside other stacks) do not collide."
  type        = string
  default     = "hello"
  validation {
    condition     = can(regex("^[a-z0-9-]{1,40}$", var.prefix))
    error_message = "prefix: 1 to 40 lowercase letters, digits or '-'."
  }
}

variable "localstack_endpoint" {
  description = "LocalStack edge URL, e.g. http://localhost:4576. Null: the provider talks to real AWS with the caller's own credentials and region."
  type        = string
  default     = null
}

variable "region" {
  description = "AWS region for every resource."
  type        = string
  default     = "us-east-1"
}

variable "test_backoff_factor" {
  description = "Test profile only: scales the consumer's retry waits (0 < factor < 1) so the DLQ arm finishes in seconds. Null keeps the real schedule (30 s, 2 m, 10 m, ...)."
  type        = number
  default     = null
}
