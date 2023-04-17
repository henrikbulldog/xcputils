""" AWS S3 file """

import os
from typing import Any
import boto3
from boto3.s3.transfer import TransferConfig

from xcputils.streaming import StreamReader, StreamWriter


class AwsS3ConnectionSettings():
    """ AWS S3 connection settings """

    def __init__(self,
                 bucket: str,
                 file_path: str,
                 aws_access_key_id: str = None,
                 aws_secret_access_key: str = None,
                 aws_session_token: str = None,
                 aws_region_name: str = None,
                 ):

        self.bucket = bucket
        self.file_path = file_path
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.aws_region_name = aws_region_name


    def get_client(self):
        """ Get S3 client """

        return boto3.Session(
            aws_access_key_id=self.aws_access_key_id
                if self.aws_access_key_id
                else os.getenv("AWS_ACCESS_KEY_ID", None),
            aws_secret_access_key=self.aws_secret_access_key
                if self.aws_secret_access_key
                else os.getenv("AWS_SECRET_ACCESS_KEY", None),
            aws_session_token=self.aws_session_token
                if self.aws_session_token
                else os.getenv("AWS_SESSION_TOKEN", None),
            region_name=self.aws_region_name
                if self.aws_region_name
                else os.getenv("AWS_DEFAULT_REGION", None)
            ).client('s3')


class AwsS3StreamReader(StreamReader):
    """ AWS S3 stream writer """

    def __init__(self, connection_settings: AwsS3ConnectionSettings):
        super().__init__()
        self.connection_settings = connection_settings


    def read(self, output_stream: Any):
        """ Read from stream """

        client = self.connection_settings.get_client()

        client.download_fileobj(
            self.connection_settings.bucket,
            self.connection_settings.file_path,
            output_stream)


class AwsS3StreamWriter(StreamWriter):
    """ AWS S3 stream writer """

    def __init__(self, connection_settings: AwsS3ConnectionSettings):
        super().__init__()
        self.connection_settings = connection_settings


    def get_file_path(self) -> str:
        """ Get filename """

        return self.connection_settings.file_path


    def set_file_path(self, file_path: str):
        """ Set filename """

        self.connection_settings.file_path = file_path


    def write(self, input_stream: Any):
        """ Write to stream """

        conf = TransferConfig(multipart_threshold=10000, max_concurrency=4)

        client = self.connection_settings.get_client()

        client.upload_fileobj(
            input_stream,
            self.connection_settings.bucket,
            self.connection_settings.file_path,
            Config=conf)
