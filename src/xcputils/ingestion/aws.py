""" Ingest from AWS S3 """

from io import BytesIO
from xcputils.ingestion import Ingestor
from xcputils.streaming import StreamWriter
from xcputils.streaming.aws import AwsS3ConnectionSettings, AwsS3StreamReader


class AwsS3Ingestor(Ingestor):
    """ Ingest from AWS S3 """

    def __init__(
        self,
        connection_settings: AwsS3ConnectionSettings,
        stream_writer: StreamWriter = None,
        ):

        super().__init__(stream_writer)
        self.connection_settings = connection_settings


    def ingest(self):
        """ Ingest """

        with BytesIO() as stream:
            AwsS3StreamReader(self.connection_settings) \
                .read(stream)
            stream.seek(0)
            self.stream_writer.write(stream)
