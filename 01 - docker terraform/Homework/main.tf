terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project = "dtc-de-course-412318"
  region  = "us-central1"
  credentials = "./../dtc-de-course-412318-e1629771e58d.json"
}



resource "google_storage_bucket" "data-lake-bucket" {
  name          = "dtc-de-course-412318-terra-bucket"
  location      = "US"

  # Optional, but recommended settings:
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}


resource "google_bigquery_dataset" "dataset" {
  dataset_id = "demo_dataset"
  project    = "dtc-de-course-412318"
  location   = "US"
}