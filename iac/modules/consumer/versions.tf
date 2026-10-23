# One provider version for every factbus IaC submodule. The provider itself
# (endpoints, credentials, region) is configured once at the call site -- a root module
# for LocalStack, a service's own root for production -- never here.

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}
