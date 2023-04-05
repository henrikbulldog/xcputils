""" Unit tests """

import io
import json
import unittest
from unittest import mock
from unittest.mock import patch
from requests import Session
from requests.auth import HTTPBasicAuth
from test.unit import mock_response
from xcputils.ingestion import http
from xcputils.streaming.string import StringStreamConnector


class TestHttpIngestor(unittest.TestCase):
    """ Test xcputils.ingest.http """


    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.stream_connector = StringStreamConnector()


    @patch.object(Session, 'get')
    def test_get(self, mock_get):
        """ Test xcputils.ingest.http.get """

        request = http.HttpRequest(url="https://mock.com/ip")

        mock_get.return_value = mock_response(json_data={"ip": "1.2.3.4"})
        ingestor = http.HttpIngestor(request=request, stream_connector=self.stream_connector)
        ingestor.ingest()
        result = self.stream_connector.read_str()
        self.assertTrue("ip" in result, f"key 'ip' not in: {result}")


    @patch.object(Session, 'get')
    def test_get_html(self, mock_get):
        """ Test xcputils.ingest.http.get HTML """

        request = http.HttpRequest(url="https://mock.com")

        mock_get.return_value = mock_response(content="<!DOCTYPE html>")
        ingestor = http.HttpIngestor(request=request, stream_connector=self.stream_connector)
        ingestor.ingest()
        result = self.stream_connector.read_str()
        self.assertEqual(result[0:15], "<!DOCTYPE html>")


    @patch.object(Session, 'get')
    def test_auth(self, mock_get):
        """ Test xcputils.ingest.http.get authentication """

        request = http.HttpRequest(
            url="https://mock.com/basic-auth",
            auth=HTTPBasicAuth('postman', 'password'))
        
        mock_get.return_value = mock_response(json_data={"authenticated": True})
        ingestor = http.HttpIngestor(request=request, stream_connector=self.stream_connector)
        ingestor.ingest()
        result = json.loads(self.stream_connector.read_str())
        self.assertEqual(result["authenticated"], True, result)


    @patch.object(Session, 'post')
    def test_post(self, mock_post):
        """ Test xcputils.ingest.http.post """

        data = {"test_key": "test_value"}

        request = http.HttpRequest(
            method=http.HttpMethod.POST,
            url="https://postman-echo.com/post",
            body=data)

        mock_post.return_value = mock_response(json_data={"data" : data})
        ingestor = http.HttpIngestor(request=request, stream_connector=self.stream_connector)
        ingestor.ingest()
        result = json.loads(self.stream_connector.read_str())
        self.assertEqual(
            result["data"],
            data,
            f"Expected to contain data: {data}: {result}")


if __name__ == '__main__':
    unittest.main()
