""" Ingestors """


from xcputils.streaming import StreamWriter
from xcputils.streaming.aws import AwsS3ConnectionSettings, AwsS3StreamWriter
from xcputils.streaming.az import AdfsConnectionSettings, AdfsStreamWriter
from xcputils.streaming.string import StringStreamWriter


class Ingestor():
    """ Ingstor base class"""

    def __init__(self, stream_writer: StreamWriter = None):
        """ Constructor """

        self.stream_writer = stream_writer


    def to_aws_s3(self, bucket: str, file_path: str):
        """ Write to AWS S3 """
        aws_s3_connection_settings = AwsS3ConnectionSettings(bucket=bucket, file_path=file_path)
        self.stream_writer = AwsS3StreamWriter(aws_s3_connection_settings)
        self.ingest()


    def to_adfs(
        self,
        container: str,
        file_name: str,
        directory: str):
        """ Write to Azure Data Lake Storage """
        adfs_connection_settings = AdfsConnectionSettings(container=container, file_name=file_name, directory=directory)
        self.stream_writer = AdfsStreamWriter(adfs_connection_settings)
        self.ingest()


    def to_string(
        self,
        ) -> str:
        """ Write to Azure Data Lake Storage """
        self.stream_writer = StringStreamWriter()
        self.ingest()
        return self.stream_writer.value


    def ingest(self):
        """ Ingest """
