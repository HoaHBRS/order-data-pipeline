resource "azurerm_data_factory" "pipeline" {
  name                = var.data_factory_name
  location            = azurerm_resource_group.pipeline.location
  resource_group_name = azurerm_resource_group.pipeline.name

  identity {
    type = "SystemAssigned"
  }

  public_network_enabled = true

  dynamic "github_configuration" {
    for_each = var.enable_github_configuration ? [1] : []

    content {
      account_name       = "HoaHBRS"
      branch_name        = "main"
      git_url            = "https://github.com"
      repository_name    = "order-data-pipeline"
      root_folder        = "/adf"
      publishing_enabled = true
    }
  }

  tags = {
    environment = var.environment
    project     = "order-data-pipeline"
  }
}