""" Ingestiors """


from xcputils.streaming import StreamConnector


class Ingestor():
    """ Ingstor base class"""

    def __init__(self, stream_connector: StreamConnector):
        """ Constructor """
        self.stream_connector = stream_connector

    def ingest(self):
        """ Ingest """
