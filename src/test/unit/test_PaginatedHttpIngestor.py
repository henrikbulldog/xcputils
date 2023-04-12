""" Unit tests """

import json
import unittest
from unittest.mock import patch
from test.unit import mock_response
from requests import Session
from xcputils.ingestion.http import HttpIngestor, HttpRequest
from xcputils.streaming.string import StringStreamWriter


class TestPaginatedHttpIngestor(unittest.TestCase):
    """ Test xcputils.ingest.http """


    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.stream_writer = StringStreamWriter()



    @patch.object(Session, "get")
    def test_get(self, mock_get):
        """ Test HttpIngestor with pagination """

        StringStreamWriter.LOG = []
        mock_get.side_effect = [
            mock_response(json_data={"data": [1, 2, 3]}),
            mock_response(json_data={"data": [4, 5, 6]}),
            mock_response(json_data={"data": [7, 8]}),
        ]

        HttpIngestor(
            http_request=HttpRequest(url="https://mock.com/ip")) \
            .with_pagination(page_size=3) \
            .write_to_string()

        print(StringStreamWriter.LOG)
        self.assertTrue(len(StringStreamWriter.LOG) == 3)
        self.assertTrue(json.loads(StringStreamWriter.LOG[0]) == {"data": [1, 2, 3]})
        self.assertTrue(json.loads(StringStreamWriter.LOG[1]) == {"data": [4, 5, 6]})
        self.assertTrue(json.loads(StringStreamWriter.LOG[2]) == {"data": [7, 8]})


if __name__ == "__main__":
    unittest.main()
