""" Azure Data Lake Storage Account Streaming Connector """

from typing import Any
import os
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from xcputils.streaming import StreamReader, StreamWriter


class AdfsConnectionSettings():
    """ Azure Sata Lake Storage connection settings """

    def __init__(self,
                 container: str,
                 file_name: str,
                 directory: str,
                 storage_account_name: str = None):

        self.container = container
        self.file_name = file_name
        self.directory = directory

        if storage_account_name is None:
            storage_account_name = os.getenv("ADFS_DEFAULT_STORAGE_ACCOUNT", None)
        self.storage_account_name = storage_account_name


    def get_client(self):
        """ get ADFS client"""

        default_credential = DefaultAzureCredential()
        return DataLakeServiceClient(
            account_url=f"https://{self.storage_account_name}.dfs.core.windows.net",
            credential=default_credential)


class AdfsStreamReader(StreamReader):
    """ Azure data Lake Storage stream reader """

    def __init__(self,
                 connection_settings: AdfsConnectionSettings):

        super().__init__()

        self.connection_settings = connection_settings


    def read(self, output_stream):
        """ Read from stream """

        client = self.connection_settings.get_client()

        file_system_client = client.get_file_system_client(
            file_system=self.connection_settings.container)

        directory_client = file_system_client.get_directory_client(
            self.connection_settings.directory)

        file_client = directory_client.get_file_client(self.connection_settings.file_name)

        downloader = file_client.download_file()

        downloader.readinto(output_stream)


class AdfsStreamWriter(StreamWriter):
    """ Azure data Lake Storage stream writer """

    def __init__(self,
                 connection_settings: AdfsConnectionSettings):

        super().__init__()

        self.connection_settings = connection_settings


    def get_filename(self) -> str:
        """ Get filename """

        return self.connection_settings.file_name


    def set_filename(self, filename: str):
        """ Set filename """

        self.connection_settings.file_name = filename


    def write(self, input_stream: Any):
        """ Write to stream """

        client = self.connection_settings.get_client()

        file_system_client = client.get_file_system_client(
            file_system=self.connection_settings.container)

        if not file_system_client.exists():

            file_system_client = client.create_file_system(
                file_system=self.connection_settings.container)

        directory_client = file_system_client.get_directory_client(
            self.connection_settings.directory)

        if not directory_client.exists():

            file_system_client.create_directory(self.connection_settings.directory)

            directory_client = file_system_client.get_directory_client(
                self.connection_settings.directory)

        file_client = directory_client.create_file(self.connection_settings.file_name)

        file_contents = input_stream.read()

        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))

        file_client.flush_data(len(file_contents))
