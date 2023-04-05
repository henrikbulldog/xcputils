""" Ingestors """


from xcputils.streaming import StreamWriter


class Ingestor():
    """ Ingstor base class"""

    def __init__(self, stream_writer: StreamWriter):
        """ Constructor """
        self.stream_writer = stream_writer

    def ingest(self):
        """ Ingest """
