# https://registry.terraform.io/providers/hashicorp/google/latest/docs
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.32.0"
    }
  }
}

# cloud overview -> dashboard -> project id
provider "google" {
# Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS set
#  credentials = "./keys/my-creds.json"
  project = "vivid-grammar-424416-a0"
  region  = "us-central1"
}


# https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "demo-bucket" {
  name          = "vivid-grammar-424416-a0" // needs to be unique
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    action {
      type = "AbortIncompleteMultipartUpload"
    }
    condition {
      age = 1  // days
    }
  }

  # Optional, but recommended settings:
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

}

