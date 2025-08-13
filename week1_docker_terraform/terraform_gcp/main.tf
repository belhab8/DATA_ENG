terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  project = "hopeful-breaker-468919-b9"
  region  = "europe-west1"
}

resource "google_storage_bucket" "demo-bucket" {
  name = "hopeful-breaker-468919-b9-terra-bucket"
  location = "EU"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}