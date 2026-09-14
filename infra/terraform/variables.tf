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

variable "key_vault_name" {
  description = "Azure Key Vault name"
  type        = string
}

variable "sql_server_name" {
  description = "Azure SQL logical server name"
  type        = string
}

variable "sql_database_name" {
  description = "Azure SQL database name"
  type        = string
}

variable "sql_entra_admin_login" {
  description = "Microsoft Entra administrator login"
  type        = string
}

variable "sql_entra_admin_object_id" {
  description = "Microsoft Entra administrator object ID"
  type        = string
}

variable "manage_runtime_resources" {
  description = "Whether Terraform creates Key Vault and SQL resources"
  type        = bool
  default     = false
}