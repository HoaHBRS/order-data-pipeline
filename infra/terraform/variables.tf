variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "Germany West Central"
}

variable "resource_group_name" {
  description = "Resource group name"
  type        = string
}

variable "storage_account_name" {
  description = "Storage account name"
  type        = string
}

variable "data_factory_name" {
  description = "Azure Data Factory name"
  type        = string
}

variable "enable_github_configuration" {
  description = "Enable GitHub integration for the Data Factory"
  type        = bool
  default     = false
}