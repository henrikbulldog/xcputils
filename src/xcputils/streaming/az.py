""" Azure Data Lake Storage Account Streaming Connector """

from typing import Any
import os
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from xcputils.streaming import StreamConnector


class AdfsStreamConnector(StreamConnector):
    """ Azure data Lake Storage stream connector """

    def __init__(self,
                 container: str,
                 directory: str,
                 file_name: str,
                 storage_account_name: str = None):

        self.container = container
        self.directory = directory
        self.file_name = file_name
        if storage_account_name == None:
            storage_account_name = os.getenv("ADFS_DEFAULT_STORAGE_ACCOUNT", None)
        default_credential = DefaultAzureCredential()
        self.adfs_client = DataLakeServiceClient(
            account_url=f"https://{storage_account_name}.dfs.core.windows.net",
            credential=default_credential)


    def read(self, output_stream: Any):
        """ Read from stream """

        file_system_client = self.adfs_client.get_file_system_client(file_system=self.container)
        directory_client = file_system_client.get_directory_client(self.directory)
        file_client = directory_client.get_file_client(self.file_name)
        downloader = file_client.download_file()
        downloader.readinto(output_stream)


    def write(self, input_stream: Any):
        """ Write to stream """

        file_system_client = self.adfs_client.get_file_system_client(file_system=self.container)
        if not file_system_client.exists():
            file_system_client = self.adfs_client.create_file_system(file_system=self.container)
        directory_client = file_system_client.get_directory_client(self.directory)
        if not directory_client.exists():
            file_system_client.create_directory(self.directory)
            directory_client = file_system_client.get_directory_client(self.directory)
        file_client = directory_client.create_file(self.file_name)
        file_contents = input_stream.read()
        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))