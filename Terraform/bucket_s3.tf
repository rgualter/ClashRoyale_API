provider "aws" {
    profile = "default"
   region = "us-east-1"
}

resource "aws_s3_bucket" "apiroyale-raw" {
    bucket = "apiroyale-raw"
    tags = {
        "API" = "ClashRoyale"
        "Layer" = "raw"
    }
}

resource "aws_s3_bucket" "apiroyale-stage" {
    bucket = "apiroyale-stage"
    tags = {
        "API" = "ClashRoyale"
        "Layer" = "stage"
    }
}

resource "aws_s3_bucket" "apiroyale-analytics" {
    bucket = "apiroyale-analytics"
    tags = {
        "API" = "ClashRoyale"
        "Layer" = "analytics"
    }
}

