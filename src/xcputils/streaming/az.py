""" Azure Data Lake Storage Account Streaming Connector """

from typing import Any
import os
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

from xcputils.streaming import StreamReader, StreamWriter


class AdfsConnectionSettings():
    """ Azure Sata Lake Storage connection settings """

    def __init__(
        self,
        container: str,
        file_name: str,
        directory: str,
        storage_account_name: str = None,
        tenant_id: str = None,
        client_id: str = None,
        client_secret: str = None,
        ):

        self.container = container
        self.file_name = file_name
        self.directory = directory
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

        if storage_account_name is None:
            storage_account_name = os.getenv("ADFS_DEFAULT_STORAGE_ACCOUNT", None)
        self.storage_account_name = storage_account_name


    def get_client(self):
        """ get ADFS client"""

        if self.tenant_id and self.client_id and self.client_secret:
            credential = ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret)
        else:
            credential = DefaultAzureCredential()

        return DataLakeServiceClient(
            account_url=f"https://{self.storage_account_name}.dfs.core.windows.net",
            credential=credential)


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


    def get_file_path(self) -> str:
        """ Get filename """

        return self.connection_settings.file_name if not self.connection_settings.directory \
            else f"{self.connection_settings.directory}/{self.connection_settings.file_name}"


    def set_file_path(self, file_path: str):
        """ Set filename """

        directory, file_name = os.path.split(file_path)

        self.connection_settings.directory = directory
        self.connection_settings.file_name = file_name


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
        file_client.upload_data(data=file_contents, overwrite=True)

