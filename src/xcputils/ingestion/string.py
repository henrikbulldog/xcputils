""" File ingestion """

from io import BytesIO
from xcputils.ingestion import Ingestor
from xcputils.streaming import StreamWriter


class StringIngestor(Ingestor):
    """ Ingest from file """

    def __init__(
        self,
        data: str,
        stream_writer: StreamWriter = None,
        ):

        super().__init__(stream_writer)

        self.data = data


    def _ingest(self):
        """ Ingest """

        with BytesIO() as stream:
            stream.write(self.data.encode('utf-8'))
            stream.seek(0)
            self.stream_writer.write(stream)
