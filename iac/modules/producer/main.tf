# A producer's AWS side: the IAM policy that lets it publish to the bus under its own
# `source` and nothing else, and, when the service runs a sweeper Lambda, the one-minute
# schedule that republishes outbox rows the in-request publish did not deliver.
#
# The policy is a JSON output so a caller can attach it however its estate attaches
# policies; `role_name` attaches it inline here.

locals {
  schedule_name = coalesce(var.name, "${replace(var.event_source, "/[^A-Za-z0-9_-]/", "-")}-sweeper")

  policy_json = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "PublishAsOwnSource"
        Effect   = "Allow"
        Action   = "events:PutEvents"
        Resource = var.bus_arn
        # PutEvents with any other Source is denied, so one service cannot publish as
        # another.
        Condition = { StringEquals = { "events:source" = var.event_source } }
      },
      {
        Sid      = "DescribeBus"
        Effect   = "Allow"
        Action   = "events:DescribeEventBus"
        Resource = var.bus_arn
      },
    ]
  })
}

resource "aws_iam_role_policy" "producer" {
  count  = var.role_name == null ? 0 : 1
  name   = "${local.schedule_name}-publish"
  role   = var.role_name
  policy = local.policy_json
}

# The sweeper schedule. A scheduled rule always lives on the account's default bus.
resource "aws_cloudwatch_event_rule" "sweeper" {
  count               = var.sweeper_function_arn == null ? 0 : 1
  name                = local.schedule_name
  description         = "Runs the ${var.event_source} outbox sweeper."
  schedule_expression = var.sweeper_schedule_expression
}

resource "aws_cloudwatch_event_target" "sweeper" {
  count = var.sweeper_function_arn == null ? 0 : 1
  rule  = aws_cloudwatch_event_rule.sweeper[0].name
  arn   = var.sweeper_function_arn
}

resource "aws_lambda_permission" "sweeper" {
  count         = var.sweeper_function_arn == null ? 0 : 1
  statement_id  = "AllowEventBridgeSweeperSchedule"
  action        = "lambda:InvokeFunction"
  function_name = var.sweeper_function_arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.sweeper[0].arn
}
