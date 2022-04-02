locals {
  data_lake_bucket = "data_expo_airline_temp1"
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources."
  default = "asia-south1"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket"
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "airline_data_temp1"
}

variable "ssh_user" {
  description = "Name of the user who is sshing into the server"
  type = string
  default = "aayush"
}

variable "ssh_pub_key_file" {
  description = "Location of the ssh public key"
}

variable "startup_script_location" {
  description = "Location of the startup script to be executed"
}