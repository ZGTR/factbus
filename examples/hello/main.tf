# factbus's runnable example: one producer source, one bus and two independent consumers
# of the same event. Each consumer has its own rule, queue, DLQs, inbox and Lambda, and
# writes its own ledger row per event, applied once through its own DynamoDB inbox. One
# consumer stopping or failing leaves the other running.
#
#   LocalStack:  tofu apply -var localstack_endpoint=http://localhost:4576
#   Real AWS:    tofu apply   (no endpoint; your credentials and region decide)
#
# No VPC, subnet, NAT gateway or database instance: the inbox and the ledger are
# DynamoDB tables.

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

locals {
  local        = var.localstack_endpoint != null
  event_source = "example.hello"
  event_type   = "hello.said"
  src_dir      = "${path.module}/../../src"
  # Two consumers of the same event, identical except for their names and state.
  consumers = toset(["ledger", "audit"])
}

provider "aws" {
  region = var.region

  # LocalStack accepts any key pair; real AWS uses the caller's credential chain.
  access_key                  = local.local ? "test" : null
  secret_key                  = local.local ? "test" : null
  skip_credentials_validation = local.local
  skip_metadata_api_check     = local.local
  skip_requesting_account_id  = local.local

  dynamic "endpoints" {
    for_each = local.local ? [var.localstack_endpoint] : []
    content {
      cloudwatch = endpoints.value
      dynamodb   = endpoints.value
      events     = endpoints.value
      iam        = endpoints.value
      lambda     = endpoints.value
      logs       = endpoints.value
      sqs        = endpoints.value
      sts        = endpoints.value
    }
  }
}

module "bus" {
  source = "../../iac/modules/bus"
  name   = "${var.prefix}-bus"
}

# No sweeper here: the e2e script publishes straight to the bus. A service with an SQL
# outbox passes its sweeper function's ARN as `sweeper_function_arn`.
module "producer" {
  source       = "../../iac/modules/producer"
  bus_arn      = module.bus.bus_arn
  event_source = local.event_source
}

# Each consumer's ledger: one item per applied event, `sk` unique per apply, so a second
# apply of the same event would show up as a second item.
resource "aws_dynamodb_table" "ledger" {
  for_each     = local.consumers
  name         = "${var.prefix}-${each.key}"
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
}

# handler.py plus the factbus package. boto3, factbus's only runtime dependency on this
# path, ships with the python3.12 Lambda runtime. Both consumers run this one artifact.
data "archive_file" "hello" {
  type        = "zip"
  output_path = "${path.module}/.build/hello.zip"

  source {
    content  = file("${path.module}/handler.py")
    filename = "handler.py"
  }

  dynamic "source" {
    for_each = fileset(local.src_dir, "factbus/**/*.py")
    content {
      content  = file("${local.src_dir}/${source.value}")
      filename = source.value
    }
  }
}

resource "aws_iam_role" "hello" {
  for_each = local.consumers
  name     = "${var.prefix}-${each.key}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# The handler's own writes and logs. The queue and inbox grants come from the consumer
# module (`role_name` below). Neither role can touch the other consumer's tables.
resource "aws_iam_role_policy" "hello" {
  for_each = local.consumers
  name     = "${var.prefix}-${each.key}-own"
  role     = aws_iam_role.hello[each.key].name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "WriteLedger"
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem"]
        Resource = aws_dynamodb_table.ledger[each.key].arn
      },
      {
        Sid      = "Logs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.hello[each.key].arn}:*"
      },
    ]
  })
}

# Owned here so `tofu destroy` removes it; Lambda would otherwise create it unmanaged.
resource "aws_cloudwatch_log_group" "hello" {
  for_each          = local.consumers
  name              = "/aws/lambda/${var.prefix}-${each.key}"
  retention_in_days = 14
}

resource "aws_lambda_function" "hello" {
  for_each         = local.consumers
  function_name    = "${var.prefix}-${each.key}"
  role             = aws_iam_role.hello[each.key].arn
  runtime          = "python3.12"
  handler          = "handler.handler"
  filename         = data.archive_file.hello.output_path
  source_code_hash = data.archive_file.hello.output_base64sha256
  timeout          = 30
  depends_on       = [aws_cloudwatch_log_group.hello]

  environment {
    variables = merge(
      {
        FACTBUS_INBOX_TABLE = "${var.prefix}-${each.key}-inbox"
        LEDGER_TABLE        = aws_dynamodb_table.ledger[each.key].name
        HELLO_SERVICE       = "${var.prefix}-${each.key}"
      },
      var.test_backoff_factor == null ? {} : { HELLO_TEST_BACKOFF_FACTOR = tostring(var.test_backoff_factor) },
    )
  }
}

# The whole consumer, once per consumer: rule, queue, DLQ, delivery DLQ, event source
# mapping, alarms, inbox table.
module "consumer" {
  source             = "../../iac/modules/consumer"
  for_each           = local.consumers
  name               = "${var.prefix}-${each.key}"
  bus_name           = module.bus.bus_name
  sources            = [local.event_source]
  event_types        = [local.event_type]
  function_arn       = aws_lambda_function.hello[each.key].arn
  role_name          = aws_iam_role.hello[each.key].name
  max_receive_count  = 3
  create_inbox_table = true
  # Named here rather than read back from the module, which would make the function
  # depend on the module that depends on the function.
  inbox_table_name = "${var.prefix}-${each.key}-inbox"

  # Short so the e2e's delivery-DLQ arm finishes in minutes; a real consumer keeps the
  # module defaults (185 attempts over 24 h).
  delivery_max_retry_attempts    = 0
  delivery_max_event_age_seconds = 60
}

# The example had one consumer before it had two. Its names are unchanged under the
# default prefix, so a stack applied from the one-consumer version (`KEEP=1 make e2e`)
# becomes the `ledger` consumer in place instead of being destroyed and recreated.
moved {
  from = aws_dynamodb_table.ledger
  to   = aws_dynamodb_table.ledger["ledger"]
}

moved {
  from = aws_iam_role.hello
  to   = aws_iam_role.hello["ledger"]
}

moved {
  from = aws_iam_role_policy.hello
  to   = aws_iam_role_policy.hello["ledger"]
}

moved {
  from = aws_cloudwatch_log_group.hello
  to   = aws_cloudwatch_log_group.hello["ledger"]
}

moved {
  from = aws_lambda_function.hello
  to   = aws_lambda_function.hello["ledger"]
}

moved {
  from = module.consumer
  to   = module.consumer["ledger"]
}
