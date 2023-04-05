""" Unit tests """

import io
import json
import unittest
from unittest import mock
from unittest.mock import patch
from requests import Session
from test.unit import mock_response
from xcputils.ingestion import http
from xcputils.streaming.string import StringStreamWriter


class TestPaginatedHttpIngestor(unittest.TestCase):
    """ Test xcputils.ingest.http """


    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.stream_writer = StringStreamWriter()



    @patch.object(Session, "get")
    def test_get(self, mock_get):
        """ Test xcputils.ingest.http.get """

        http_request = http.HttpRequest(url="https://mock.com/ip")
        
        StringStreamWriter.LOG = []
        mock_get.side_effect = [
            mock_response(json_data={"data": [1, 2, 3]}),
            mock_response(json_data={"data": [4, 5, 6]}),
            mock_response(json_data={"data": [7, 8]}),
        ]
        pagination_handler = http.PaginationHandler(page_size=3)
        ingestor = http.PaginatedHttpIngestor(
            http_request=http_request,
            pagination_handler=pagination_handler,
            get_stream_writer=lambda page_number: self.stream_writer)
        ingestor.ingest()
        print(StringStreamWriter.LOG)
        self.assertTrue(len(StringStreamWriter.LOG) == 3)
        self.assertTrue(json.loads(StringStreamWriter.LOG[0]) == {"data": [1, 2, 3]})
        self.assertTrue(json.loads(StringStreamWriter.LOG[1]) == {"data": [4, 5, 6]})
        self.assertTrue(json.loads(StringStreamWriter.LOG[2]) == {"data": [7, 8]})


if __name__ == "__main__":
    unittest.main()