""" Ingestors """


from xcputils.streaming import StreamWriter
from xcputils.streaming.aws import AwsS3ConnectionSettings, AwsS3StreamWriter
from xcputils.streaming.az import AdfsConnectionSettings, AdfsStreamWriter
from xcputils.streaming.file import FileStreamWriter
from xcputils.streaming.string import StringStreamWriter


class Ingestor():
    """ Ingstor base class"""

    def __init__(self, stream_writer: StreamWriter = None):
        """ Constructor """

        self.stream_writer = stream_writer


    def write_to_aws_s3(
        self,
        bucket: str,
        file_path: str,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        aws_session_token: str = None,
        aws_region_name: str = None):
        """ Write to AWS S3 """

        aws_s3_connection_settings = AwsS3ConnectionSettings(
            bucket=bucket,
            file_path=file_path,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            aws_region_name=aws_region_name)

        self.stream_writer = AwsS3StreamWriter(aws_s3_connection_settings)

        self.ingest()


    def write_to_adfs(
        self,
        container: str,
        file_name: str,
        directory: str,
        storage_account_name: str = None,
        tenant_id: str = None,
        client_id: str = None,
        client_secret: str = None,
        ):
        """ Write to Azure Data Lake Storage """
        adfs_connection_settings = AdfsConnectionSettings(
            container=container,
            file_name=file_name,
            directory=directory,
            storage_account_name=storage_account_name,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            )

        self.stream_writer = AdfsStreamWriter(adfs_connection_settings)

        self.ingest()


    def write_to_string(
        self,
        ) -> str:
        """ Write to Azure Data Lake Storage """

        self.stream_writer = StringStreamWriter()

        self.ingest()

        return self.stream_writer.value


    def write_to_file(self, file_path: str):
        """ Write to file """

        self.stream_writer = FileStreamWriter(file_path=file_path)

        self.ingest()


    def ingest(self):
        """ Ingest """
