terraform {
  required_version = ">= 1.15.0, < 2.0.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }

  backend "azurerm" {
    use_cli              = true
    use_azuread_auth     = true
    storage_account_name = "storderpipelinedev270826"
    container_name       = "tfstate"
    key                  = "order-data-pipeline-dev.tfstate"
  }
}
