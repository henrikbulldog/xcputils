""" Package xcputils """

from xcputils.ingestion.aws import AwsS3Ingestor
from xcputils.ingestion.az import AdfsIngestor
from xcputils.ingestion.file import FileIngestor
from xcputils.ingestion.http import HttpIngestor, HttpMethod, HttpRequest
from xcputils.streaming.aws import AwsS3ConnectionSettings
from xcputils.streaming.az import AdfsConnectionSettings


class XCPUtils():
    """ Utilities for copying data """

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
