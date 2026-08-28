resource "azurerm_storage_container" "raw" {
  name                  = "raw"
  storage_account_id    = azurerm_storage_account.pipeline.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "processed" {
  name                  = "processed"
  storage_account_id    = azurerm_storage_account.pipeline.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "rejected" {
  name                  = "rejected"
  storage_account_id    = azurerm_storage_account.pipeline.id
  container_access_type = "private"
}