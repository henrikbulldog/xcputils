""" Unit tests """

import io
import json
import unittest
from unittest import mock
from unittest.mock import patch
from requests import Session
from test.unit import mock_response
from xcputils.ingestion import http
from xcputils.streaming.string import StringStreamConnector


class TestPaginatedHttpIngestor(unittest.TestCase):
    """ Test xcputils.ingest.http """


    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.stream_connector = StringStreamConnector()



    @patch.object(Session, "get")
    def test_get(self, mock_get):
        """ Test xcputils.ingest.http.get """

        request = http.HttpRequest(url="https://mock.com/ip")
        
        StringStreamConnector.LOG = []
        mock_get.side_effect = [
            mock_response(json_data={"data": [1, 2, 3]}),
            mock_response(json_data={"data": [4, 5, 6]}),
            mock_response(json_data={"data": [7, 8]}),
        ]
        handler = http.PaginationHandler(page_size=3)
        ingestor = http.PaginatedHttpIngestor(
            request=request,
            stream_connector=self.stream_connector,
            handler=handler)
        ingestor.ingest()
        print(StringStreamConnector.LOG)
        self.assertTrue(len(StringStreamConnector.LOG) == 3)
        self.assertTrue(json.loads(StringStreamConnector.LOG[0]) == {"data": [1, 2, 3]})
        self.assertTrue(json.loads(StringStreamConnector.LOG[1]) == {"data": [4, 5, 6]})
        self.assertTrue(json.loads(StringStreamConnector.LOG[2]) == {"data": [7, 8]})


if __name__ == "__main__":
    unittest.main()
