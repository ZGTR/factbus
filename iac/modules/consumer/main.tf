# One consumer's whole AWS side in one module block: an EventBridge rule on the
# producer's bus, the queue and DLQ it delivers to, the Lambda event source mapping that
# drains the queue, three alarms and, optionally, the DynamoDB inbox table the kit uses
# to apply each event once. No VPC, subnet, NAT gateway or database instance: the inbox
# is DynamoDB, so a consumer needs no network plumbing.
#
# One bus and one rule per consumer, so a caller writes about ten lines. A consumer of
# several buses instantiates the module once per bus.

locals {
  # The function name, for the Lambda Errors alarm. A full ARN carries it as the 7th
  # field; a bare name or a qualified ARN is handled too.
  function_name = (
    length(split(":", var.function_arn)) >= 7 ? split(":", var.function_arn)[6] : var.function_arn
  )
  alarm_actions    = var.alarm_topic_arn == null ? [] : [var.alarm_topic_arn]
  inbox_table_name = coalesce(var.inbox_table_name, "${var.name}-inbox")
}

resource "aws_cloudwatch_event_rule" "this" {
  name           = var.name
  event_bus_name = var.bus_name
  event_pattern = jsonencode({
    source        = var.sources
    "detail-type" = var.event_types
  })
}

resource "aws_cloudwatch_event_target" "this" {
  rule           = aws_cloudwatch_event_rule.this.name
  event_bus_name = var.bus_name
  arn            = aws_sqs_queue.this.arn
}

resource "aws_sqs_queue" "dlq" {
  name                      = "${var.name}-dlq"
  message_retention_seconds = var.retention_seconds
}

resource "aws_sqs_queue" "this" {
  name                       = var.name
  message_retention_seconds  = var.retention_seconds
  visibility_timeout_seconds = var.function_timeout_seconds * 6

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.max_receive_count
  })
}

# The queue's only policy. It admits this module's rule and nothing else: no wildcard,
# no account-wide grant, no ARN from outside.
resource "aws_sqs_queue_policy" "this" {
  queue_url = aws_sqs_queue.this.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowEventBridgeRule"
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.this.arn
      Condition = { ArnEquals = { "aws:SourceArn" = aws_cloudwatch_event_rule.this.arn } }
    }]
  })
}

# `enabled` is the kill switch (docs/kill-switch-runbook.md). Setting it false in the
# caller's IaC keeps a thrown switch off across later applies; messages wait on the
# queue for up to `retention_seconds`.
resource "aws_lambda_event_source_mapping" "this" {
  event_source_arn                   = aws_sqs_queue.this.arn
  function_name                      = var.function_arn
  enabled                            = var.enabled
  batch_size                         = var.batch_size
  maximum_batching_window_in_seconds = var.maximum_batching_window_in_seconds
  function_response_types            = ["ReportBatchItemFailures"]
}

resource "aws_cloudwatch_metric_alarm" "dlq_visible" {
  alarm_name          = "${var.name}-dlq-visible"
  alarm_description   = "A message reached ${var.name}'s DLQ after ${var.max_receive_count} receives."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.dlq.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "queue_age" {
  alarm_name          = "${var.name}-age"
  alarm_description   = "The oldest message on ${var.name} is older than ${var.age_alarm_threshold_seconds} s."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  dimensions          = { QueueName = aws_sqs_queue.this.name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = var.age_alarm_threshold_seconds
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${var.name}-errors"
  alarm_description   = "The function draining ${var.name} raised an error."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = local.function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}

# The inbox: one item per applied event, written with a conditional put so a redelivery
# is recognised and skipped. `expires_at` (epoch seconds) lets DynamoDB drop old keys.
resource "aws_dynamodb_table" "inbox" {
  count        = var.create_inbox_table ? 1 : 0
  name         = local.inbox_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }
}

locals {
  consumer_policy_json = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [{
        Sid    = "DrainQueue"
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:ChangeMessageVisibility",
          "sqs:GetQueueAttributes",
        ]
        Resource = aws_sqs_queue.this.arn
      }],
      var.create_inbox_table ? [{
        Sid    = "UseInbox"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:ConditionCheckItem",
        ]
        Resource = aws_dynamodb_table.inbox[0].arn
      }] : [],
    )
  })
}

# Optional: attach the policy to the consumer function's role. A role name written in
# the caller's config is known at plan; a generated one (name_prefix) is not, and then
# the caller attaches `consumer_policy_json` itself.
resource "aws_iam_role_policy" "consumer" {
  count  = var.role_name == null ? 0 : 1
  name   = "${var.name}-consumer"
  role   = var.role_name
  policy = local.consumer_policy_json
}
