resource "azurerm_resource_group" "pipeline" {
  name     = "rg-order-data-pipeline-dev"
  location = "Germany West Central"

  tags = {
    environment = "dev"
    project     = "order-data-pipeline"
  }
}