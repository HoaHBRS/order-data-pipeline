data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "pipeline" {
  count = var.manage_runtime_resources ? 1 : 0

  name                = var.key_vault_name
  location            = azurerm_resource_group.pipeline.location
  resource_group_name = azurerm_resource_group.pipeline.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  soft_delete_retention_days = 7
  purge_protection_enabled   = false

  tags = {
    environment = var.environment
    project     = "order-data-pipeline"
  }
}

resource "azurerm_key_vault_access_policy" "data_factory" {
  count = var.manage_runtime_resources ? 1 : 0

  key_vault_id = azurerm_key_vault.pipeline[0].id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = azurerm_data_factory.pipeline.identity[0].principal_id

  secret_permissions = [
    "Get",
    "List",
  ]
}