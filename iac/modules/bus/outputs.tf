output "bus_name" {
  value = aws_cloudwatch_event_bus.this.name
}

output "bus_arn" {
  value = aws_cloudwatch_event_bus.this.arn
}

output "archive_arn" {
  value = aws_cloudwatch_event_archive.this.arn
}
