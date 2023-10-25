""" File ingestion """

from xcputils.ingestion import Ingestor
from xcputils.streaming import StreamWriter


class FileIngestor(Ingestor):
    """ Ingest from file """

    def __init__(
        self,
        file_path: str,
        stream_writer: StreamWriter = None,
        ):

        super().__init__(stream_writer)

        self.file_path = file_path


    def ingest(self):
        """ Ingest """

        with open(self.file_path, "rb") as file_stream:
            self.stream_writer.write(file_stream)
