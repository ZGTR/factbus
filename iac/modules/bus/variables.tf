variable "name" {
  description = "The bus name."
  type        = string
  validation {
    condition     = length(var.name) > 0
    error_message = "name must not be empty: an empty bus name means the account's default bus."
  }
}

variable "archive_retention_days" {
  description = "How long the archive keeps events for replay (P2, P3). Must stay below every consumer's inbox retention (35 days by default)."
  type        = number
  default     = 30
  validation {
    condition     = var.archive_retention_days > 0 && var.archive_retention_days < 35
    error_message = "archive_retention_days must be 1-34: a replay older than the inbox retention would apply events again."
  }
}
