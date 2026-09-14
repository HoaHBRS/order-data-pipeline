resource "azurerm_resource_group" "pipeline" {
  name     = var.resource_group_name
  location = var.location

  tags = {
    environment = var.environment
    project     = "order-data-pipeline"
    managed_by  = "terraform"
  }
}
