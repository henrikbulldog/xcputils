""" Unit tests """

import json
import os
import unittest
from requests.auth import HTTPBasicAuth
from xcputils.ingestion import http
from xcputils.streaming.aws import S3StreamConnector
from xcputils.streaming.az import AdfsStreamConnector

class TestHttpIngestor(unittest.TestCase):
    """ Test xcputils.ingest.http """


    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.stream_connectors = [
            S3StreamConnector(
                container=os.environ['AWS_S3_BUCKET'],
                file_name="test.txt",
                directory="tests3streamconnector/folder"
                ),
            AdfsStreamConnector(
                container="testadfsstreamconnector",
                file_name="test.txt",
                directory="folder"
                ),
        ]


    def test_get(self):
        """ Test xcputils.ingest.http.get """

        request = http.HttpRequest(url="https://postman-echo.com/ip")

        for stream_connector in self.stream_connectors:
            with self.subTest(msg=str(type(stream_connector))):
                ingestor = http.HttpIngestor(request=request, stream_connector=stream_connector)
                ingestor.ingest()
                result = stream_connector.read_str()
                self.assertTrue("ip" in result, f"key 'ip' not in: {result}")


    def test_get_html(self):
        """ Test xcputils.ingest.http.get HTML """

        request = http.HttpRequest(url="https://postman-echo.com")

        for stream_connector in self.stream_connectors:
            with self.subTest():
                ingestor = http.HttpIngestor(request=request, stream_connector=stream_connector)
                ingestor.ingest()
                result = stream_connector.read_str()
                self.assertEqual(result[0:15], "<!DOCTYPE html>")


    def test_auth(self):
        """ Test xcputils.ingest.http.get authentication """

        request = http.HttpRequest(
            url="https://postman-echo.com/basic-auth",
            auth=HTTPBasicAuth('postman', 'password'))
        
        for stream_connector in self.stream_connectors:
            with self.subTest():
                ingestor = http.HttpIngestor(request=request, stream_connector=stream_connector)
                ingestor.ingest()
                result = json.loads(stream_connector.read_str())
                self.assertEqual(result["authenticated"], True, result)


    def test_post(self):
        """ Test xcputils.ingest.http.post """

        data = {"test_key": "test_value"}

        request = http.HttpRequest(
            method=http.HttpMethod.POST,
            url="https://postman-echo.com/post",
            body=data)

        for stream_connector in self.stream_connectors:
            with self.subTest():
                ingestor = http.HttpIngestor(request=request, stream_connector=stream_connector)
                ingestor.ingest()
                result = json.loads(stream_connector.read_str())
                self.assertEqual(
                    result["data"],
                    data,
                    f"Expected to contain data: {data}: {result}")


if __name__ == '__main__':
    unittest.main()
