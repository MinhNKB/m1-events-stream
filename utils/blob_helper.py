from azure.storage.blob import BlobServiceClient # azure-storage-blob
import pandas as pd
from io import BytesIO

class BlobHelper:
    def __init__(self, blob_account, blob_key):
        connection_string = self.build_blob_connection_string(blob_account, blob_key)
        self.start_blob_service_client(connection_string)

    def start_blob_service_client(self, connection_string):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    def build_blob_connection_string(self, blob_account, blob_key):
        return 'DefaultEndpointsProtocol=https;AccountName={};AccountKey={};EndpointSuffix=core.windows.net'.format(blob_account, blob_key)

    def get_blob_container_client(self, blob_container):
        return self.blob_service_client.get_container_client(blob_container)

    def get_blob_client(self, blob_container, file_path):
        return self.blob_service_client.get_blob_client(container=blob_container, blob=file_path)

    def upload_data(self, data, blob_container, file_path, overwrite=False):
        blob_client = self.get_blob_client(blob_container, file_path)
        blob_client.upload_blob(data, overwrite=overwrite)

    def download_data(self, blob_container, file_path):
        blob_client = self.get_blob_client(blob_container, file_path)
        return blob_client.download_blob().readall()