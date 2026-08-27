import os
import csv
from io import StringIO
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential


def create_blob_service_client():
    connection_string = os.getenv(
        "AZURE_STORAGE_CONNECTION_STRING"
    )

    if connection_string:
        return BlobServiceClient.from_connection_string(
            connection_string
        )

    account_url = os.environ[
        "AZURE_STORAGE_ACCOUNT_URL"
    ]

    return BlobServiceClient(
        account_url=account_url,
        credential=DefaultAzureCredential(),
    )


def read_blob_text(container_name, blob_name):

    blob_service_client = create_blob_service_client()

    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name,
    )

    return blob_client.download_blob().readall().decode("utf-8")


def upload_rows_as_csv(
    container_name,
    blob_name,
    rows,
    fieldnames,
):
    csv_buffer = StringIO()
    writer = csv.DictWriter(
        csv_buffer,
        fieldnames=fieldnames,
    )
    writer.writeheader()
    writer.writerows(rows)

    blob_service_client = create_blob_service_client()
    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name,
    )

    blob_client.upload_blob(
        csv_buffer.getvalue(),
        overwrite=True,
    )

if __name__ == "__main__":
    print(read_blob_text("raw", "orders_2026-08-01.csv"))