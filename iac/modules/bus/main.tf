# The event bus and its archive (design §1). The archive lets an operator replay a
# window of history into a consumer; the inbox absorbs the replay rather than applying
# it twice (C0b-5, F11). Keep `archive_retention_days` below the consumers' inbox
# retention (35 days by default), or a replay of older events applies them again.

resource "aws_cloudwatch_event_bus" "this" {
  name = var.name
}

resource "aws_cloudwatch_event_archive" "this" {
  name             = "${var.name}-archive"
  event_source_arn = aws_cloudwatch_event_bus.this.arn
  retention_days   = var.archive_retention_days
}
