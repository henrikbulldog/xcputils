""" Package xcputils """

from xcputils.ingestion.http import HttpIngestor


class XCPUtils():
    """ Utilities for copying data """


    def from_http(self, url: str):
        """ Ingest from HTTP """

        return HttpIngestor(url=url)
