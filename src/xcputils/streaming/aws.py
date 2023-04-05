""" String writer """

import os
from typing import Any
import boto3
from boto3.s3.transfer import TransferConfig

from xcputils.streaming import StreamConnector


class S3StreamConnector(StreamConnector):
    """ AWS S3 stream connector """


    def __init__(self,
                 container: str, 
                 file_name: str,
                 directory: str = "",
                 aws_access_key_id: str = None,
                 aws_secret_access_key: str = None,
                 aws_session_token: str = None,
                 aws_region_name: str = None):

        super().__init__(container=container, file_name=file_name, directory=directory)

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.aws_region_name = aws_region_name


    def __get_client(self):
        return boto3.Session(
            aws_access_key_id=self.aws_access_key_id if self.aws_access_key_id
                else os.getenv("AWS_ACCESS_KEY_ID", None),
            aws_secret_access_key=self.aws_secret_access_key if self.aws_secret_access_key
                else os.getenv("AWS_SECRET_ACCESS_KEY", None),
            aws_session_token=self.aws_session_token if self.aws_session_token
                else os.getenv("AWS_SESSION_TOKEN", None),
            region_name=self.aws_region_name if self.aws_region_name
                else os.getenv("AWS_DEFAULT_REGION", None)
            ).client('s3')


    def __get_file_path(self):
        if self.directory:
            return f"{self.directory}/{self.file_name}"
        else:
            return self.file_name


    def read(self, output_stream: Any):
        """ Read from stream """

        client = self.__get_client()
        client.download_fileobj(self.container, self.__get_file_path(), output_stream)
        client.close()



    def write(self, input_stream: Any):
        """ Write to stream """
        conf = TransferConfig(multipart_threshold=10000, max_concurrency=4)
        client = self.__get_client()
        client.upload_fileobj(input_stream, self.container, self.__get_file_path(), Config=conf)
        client.close()
 