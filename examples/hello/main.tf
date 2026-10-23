# factbus's runnable example: one producer source, one bus, one consumer Lambda that
# writes a ledger row per event, applied once through the DynamoDB inbox.
#
#   LocalStack:  tofu apply -var localstack_endpoint=http://localhost:4566
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
  name   = "hello-bus"
}

# No sweeper here: the e2e script publishes straight to the bus. A service with an SQL
# outbox passes its sweeper function's ARN as `sweeper_function_arn`.
module "producer" {
  source       = "../../iac/modules/producer"
  bus_arn      = module.bus.bus_arn
  event_source = local.event_source
}

# The ledger the handler writes: one item per applied event, `sk` unique per apply, so a
# second apply of the same event would show up as a second item.
resource "aws_dynamodb_table" "ledger" {
  name         = "hello-ledger"
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
# path, ships with the python3.12 Lambda runtime.
data "archive_file" "hello" {
  type        = "zip"
  output_path = "${path.module}/.build/hello-ledger.zip"

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
  name = "hello-ledger"
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
# module (`role_name` below).
resource "aws_iam_role_policy" "hello" {
  name = "hello-ledger-own"
  role = aws_iam_role.hello.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "WriteLedger"
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem"]
        Resource = aws_dynamodb_table.ledger.arn
      },
      {
        Sid      = "Logs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "*"
      },
    ]
  })
}

# Owned here so `tofu destroy` removes it; Lambda would otherwise create it unmanaged.
resource "aws_cloudwatch_log_group" "hello" {
  name              = "/aws/lambda/hello-ledger"
  retention_in_days = 14
}

resource "aws_lambda_function" "hello" {
  function_name    = "hello-ledger"
  role             = aws_iam_role.hello.arn
  runtime          = "python3.12"
  handler          = "handler.handler"
  filename         = data.archive_file.hello.output_path
  source_code_hash = data.archive_file.hello.output_base64sha256
  timeout          = 30
  depends_on       = [aws_cloudwatch_log_group.hello]

  environment {
    variables = merge(
      {
        FACTBUS_INBOX_TABLE = module.consumer.inbox_table_name
        LEDGER_TABLE      = aws_dynamodb_table.ledger.name
      },
      var.test_backoff_factor == null ? {} : { HELLO_TEST_BACKOFF_FACTOR = tostring(var.test_backoff_factor) },
    )
  }
}

# The whole consumer: rule, queue, DLQ, event source mapping, alarms, inbox table.
module "consumer" {
  source             = "../../iac/modules/consumer"
  name               = "hello-ledger"
  bus_name           = module.bus.bus_name
  sources            = [local.event_source]
  event_types        = [local.event_type]
  function_arn       = aws_lambda_function.hello.arn
  role_name          = aws_iam_role.hello.name
  max_receive_count  = 3
  create_inbox_table = true
}
