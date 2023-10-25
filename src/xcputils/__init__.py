""" Package xcputils """

from xcputils.checkpoints import Checkpoint
from xcputils.checkpoints.aws import AwsS3Checkpoint
from xcputils.checkpoints.az import AdfsCheckpoint
from xcputils.checkpoints.file import FileCheckpoint
from xcputils.ingestion.aws import AwsS3Ingestor
from xcputils.ingestion.az import AdfsIngestor
from xcputils.ingestion.file import FileIngestor
from xcputils.ingestion.ftp import FtpIngestor
from xcputils.ingestion.http import HttpIngestor, HttpMethod, HttpRequest
from xcputils.ingestion.string import StringIngestor
from xcputils.streaming.aws import AwsS3ConnectionSettings
from xcputils.streaming.az import AdfsConnectionSettings


class XCPUtils():
    """ Utilities for copying data """


    def read_from_string(self, data: str) -> StringIngestor:
        """ Ingest from string """

        return StringIngestor(data)


    def read_from_file(self, file_path: str) -> FileIngestor:
        """ Ingest from file """

        return FileIngestor(file_path=file_path)


    def read_from_http(
        self,
        url: str,
        method: HttpMethod = HttpMethod.GET,
        params: dict = None,
        body: dict = None,
        headers: dict = None,
        auth = None,
        ) -> HttpIngestor:
        """ Ingest from HTTP """

        return HttpIngestor(
            http_request=HttpRequest(
                url=url,
                method=method,
                params=params,
                body=body,
                headers=headers,
                auth=auth)
        )


    def read_from_aws_s3(
        self,
        bucket: str,
        file_path: str,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        aws_session_token: str = None,
        aws_region_name: str = None,
    ) -> AwsS3Ingestor:
        """ Ingest from AWS S3 """

        return AwsS3Ingestor(
            connection_settings=AwsS3ConnectionSettings(
                bucket=bucket,
                file_path=file_path,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                aws_region_name=aws_region_name,
            )
        )


    def read_from_adfs(
        self,
        container: str,
        file_name: str,
        directory: str,
        storage_account_name: str = None,
        tenant_id: str = None,
        client_id: str = None,
        client_secret: str = None,
    ) -> AdfsIngestor:
        """ Ingest from AWS S3 """

        return AdfsIngestor(
            connection_settings=AdfsConnectionSettings(
                container=container,
                file_name=file_name,
                directory=directory,
                storage_account_name=storage_account_name,
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
        )


    def read_from_ftp(
        self,
        url: str,
        folder: str = None,
        file_name: str = None,
        user: str = None,
        password: str = None,
    ) -> FtpIngestor:
        """ Ingest frm FTP """

        return FtpIngestor(
            url=url,
            folder=folder,
            file_name=file_name,
            user=user,
            password=password,
            )

    def get_checkpoint_file(self, name: str, directory: str) -> Checkpoint:
        """ Get file checkpoint """

        return FileCheckpoint(name=name,directory=directory)


    def get_checkpoint_adfs(
        self, 
        name: str,
        container: str,
        directory: str,
        storage_account_name: str = None,
        tenant_id: str = None,
        client_id: str = None,
        client_secret: str = None,
        ) -> Checkpoint:
        """ Get ADFS checkpoint """

        return AdfsCheckpoint(
            name=name,
            container=container,
            directory=directory,
            storage_account_name=storage_account_name,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )


    def get_checkpoint_awss3(
        self, 
        name: str,
        bucket: str,
        directory: str,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        aws_session_token: str = None,
        aws_region_name: str = None,
        ):
        """ Get AWS S3 checkpoint """

        return AwsS3Checkpoint(
            name=name,
            bucket=bucket,
            directory=directory,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            aws_region_name=aws_region_name,
        )
