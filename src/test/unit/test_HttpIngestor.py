""" Unit tests """

import json
import unittest
from unittest.mock import patch
from test.unit import mock_response
from requests.auth import HTTPBasicAuth
from requests import Session
from xcputils.ingestion import http
from xcputils.streaming.string import StringStreamWriter


class TestHttpIngestor(unittest.TestCase):
    """ Test xcputils.ingest.http """


    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)


    @patch.object(Session, 'get')
    def test_get(self, mock_get):
        """ Test xcputils.ingest.http.get """

        mock_get.return_value = mock_response(json_data={"ip": "1.2.3.4"})
        result = http.HttpIngestor(url="https://mock.com/ip") \
            .to_string()
        self.assertTrue("ip" in result, f"key 'ip' not in: {result}")


    @patch.object(Session, 'get')
    def test_get_html(self, mock_get):
        """ Test xcputils.ingest.http.get HTML """

        mock_get.return_value = mock_response(content="<!DOCTYPE html>")
        result = http.HttpIngestor(url="https://mock.com") \
            .to_string()
        self.assertEqual(result[0:15], "<!DOCTYPE html>")


    @patch.object(Session, 'get')
    def test_auth(self, mock_get):
        """ Test xcputils.ingest.http.get authentication """

        mock_get.return_value = mock_response(json_data={"authenticated": True})
        result = http.HttpIngestor(
            url="https://mock.com/basic-auth",
            auth=HTTPBasicAuth('postman', 'password')) \
            .to_string()
        result = json.loads(result)
        self.assertEqual(result["authenticated"], True, result)


    @patch.object(Session, 'post')
    def test_post(self, mock_post):
        """ Test xcputils.ingest.http.post """

        data = {"test_key": "test_value"}

        mock_post.return_value = mock_response(json_data={"data" : data})
        result = http.HttpIngestor(
            url="https://mock.com/post",
            method=http.HttpMethod.POST,
            body=data) \
            .to_string()
        result = json.loads(result)
        self.assertEqual(
            result["data"],
            data,
            f"Expected to contain data: {data}: {result}")


if __name__ == '__main__':
    unittest.main()
