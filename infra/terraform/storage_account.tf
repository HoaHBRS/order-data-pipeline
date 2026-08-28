resource "azurerm_storage_account" "pipeline" {
  name                     = "storderpipelinedev270826"
  resource_group_name      = azurerm_resource_group.pipeline.name
  location                 = azurerm_resource_group.pipeline.location
  account_kind             = "StorageV2"
  account_tier             = "Standard"
  account_replication_type = "LRS"
  access_tier              = "Hot"

  https_traffic_only_enabled       = true
  min_tls_version                  = "TLS1_2"
  allow_nested_items_to_be_public  = false
  public_network_access_enabled    = true
  shared_access_key_enabled        = true
  cross_tenant_replication_enabled = false
  allowed_copy_scope               = "AAD"

  blob_properties {
    delete_retention_policy {
      days = 7
    }

    container_delete_retention_policy {
      days = 7
    }
  }

  share_properties {
    retention_policy {
      days = 7
    }
  }

  tags = {
    environment = "dev"
    project     = "order-data-pipeline"
  }
}