resource "azurerm_mssql_server" "pipeline" {
  count = var.manage_runtime_resources ? 1 : 0

  name                          = var.sql_server_name
  resource_group_name           = azurerm_resource_group.pipeline.name
  location                      = azurerm_resource_group.pipeline.location
  version                       = "12.0"
  minimum_tls_version           = "1.2"
  public_network_access_enabled = true

  azuread_administrator {
    login_username              = var.sql_entra_admin_login
    object_id                   = var.sql_entra_admin_object_id
    tenant_id                   = data.azurerm_client_config.current.tenant_id
    azuread_authentication_only = true
  }

  tags = {
    environment = var.environment
    project     = "order-data-pipeline"
  }
}

resource "azurerm_mssql_database" "pipeline" {
  count = var.manage_runtime_resources ? 1 : 0

  name      = var.sql_database_name
  server_id = azurerm_mssql_server.pipeline[0].id

  sku_name                    = "GP_S_Gen5_2"
  min_capacity                = 0.5
  auto_pause_delay_in_minutes = 60
  max_size_gb                 = 32
  license_type                = "LicenseIncluded"
  storage_account_type        = "Local"
  zone_redundant              = false

  tags = {
    environment = var.environment
    project     = "order-data-pipeline"
  }
}

resource "azurerm_mssql_firewall_rule" "allow_azure_services" {
  count = var.manage_runtime_resources ? 1 : 0

  name      = "AllowAzureServices"
  server_id = azurerm_mssql_server.pipeline[0].id

  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}