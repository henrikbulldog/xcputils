""" Ingest from AWS S3 """

from xcputils.ingestion import Ingestor
from xcputils.streaming import StreamWriter
from xcputils.streaming.az import AdfsConnectionSettings, AdfsStreamReader


class AdfsIngestor(Ingestor):
    """ Ingest from AWS S3 """

    def __init__(
        self,
        connection_settings: AdfsConnectionSettings,
        stream_writer: StreamWriter = None,
        ):

        super().__init__(stream_writer)
        self.connection_settings = connection_settings


    def _ingest(self):
        """ Ingest """

        AdfsStreamReader(self.connection_settings) \
            .read(self.stream_writer)
        
