variable "creds" {
  description = "GCP Project Creds"
  default     = "../../../my-creds.json"
}

variable "project_id" {
  description = "My Google Cloud Project ID for DE Zoomcamp week 1."
  default     = "hopeful-breaker-468919-b9"
}

variable "location" {
  description = "Region value for DE Zoomcamp week 1."
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My Big Query Dataset Name for DE Zoomcamp week 1."
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Google Cloud Bucket Name for DE Zoomcamp week 1."
  default     = "hopeful-breaker-468919-b9-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage class"
  default     = "STANDARD"
}

