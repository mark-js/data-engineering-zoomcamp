variable "project" {
  description = "Project id"
  type        = string
}

variable "region" {
  description = "Region"
  type        = string
}

variable "bq_dataset_name" {
  description = "BigQuery dataset name"
  type        = string
}

variable "gcs_bucket_name" {
  description = "Storage bucket name"
  type        = string
}

variable "gcs_storage_class" {
  description = "Bucket storage class"
  type        = string
  default     = "STANDARD"
}