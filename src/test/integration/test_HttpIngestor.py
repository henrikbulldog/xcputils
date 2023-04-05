""" Unit tests """

import json
import os
import unittest
from requests.auth import HTTPBasicAuth
from xcputils.ingestion import http
from xcputils.streaming.aws import AwsS3StreamReader, AwsS3StreamWriter, AwsS3ConnectionSettings
from xcputils.streaming.az import AdfsStreamReader, AdfsStreamWriter, AdfsConnectionSettings

class TestHttpIngestor(unittest.TestCase):
    """ Test xcputils.ingest.http """


    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)

        aws_connection_settings = AwsS3ConnectionSettings(
            bucket=os.environ['AWS_S3_BUCKET'],
            file_path="tests3streamconnector/folder/test.txt"
            )

        adfs_connection_Settings = AdfsConnectionSettings(
            container="testadfsstreamconnector",
            file_name="test.txt",
            directory="folder"
            )

        self.platforms = [
            { 
                "name": "AWS S3",
                "stream_writer": AwsS3StreamWriter(aws_connection_settings),
                "stream_reader": AwsS3StreamReader(aws_connection_settings),
            },
            { 
                "name": "ADFS",
                "stream_writer": AdfsStreamWriter(adfs_connection_Settings),
                "stream_reader": AdfsStreamReader(adfs_connection_Settings),
            },
        ]


    def test_get(self):
        """ Test xcputils.ingest.http.get """

        http_request = http.HttpRequest(url="https://postman-echo.com/ip")

        for platform in self.platforms:
            with self.subTest(msg=platform["name"]):
                ingestor = http.HttpIngestor(http_request, platform["stream_writer"])
                ingestor.ingest()
                result = platform["stream_reader"].read_str()
                self.assertTrue("ip" in result, f"key 'ip' not in: {result}")


    def test_get_html(self):
        """ Test xcputils.ingest.http.get HTML """

        http_request = http.HttpRequest(url="https://postman-echo.com")

        for platform in self.platforms:
            with self.subTest(msg=platform["name"]):
                ingestor = http.HttpIngestor(http_request, platform["stream_writer"])
                ingestor.ingest()
                result = platform["stream_reader"].read_str()
                self.assertEqual(result[0:15], "<!DOCTYPE html>")


    def test_auth(self):
        """ Test xcputils.ingest.http.get authentication """

        http_request = http.HttpRequest(
            url="https://postman-echo.com/basic-auth",
            auth=HTTPBasicAuth('postman', 'password'))
        
        for platform in self.platforms:
            with self.subTest(msg=platform["name"]):
                ingestor = http.HttpIngestor(http_request, platform["stream_writer"])
                ingestor.ingest()
                result = platform["stream_reader"].read_str()
                result = json.loads(result)
                self.assertEqual(result["authenticated"], True, result)


    def test_post(self):
        """ Test xcputils.ingest.http.post """

        data = {"test_key": "test_value"}

        http_request = http.HttpRequest(
            method=http.HttpMethod.POST,
            url="https://postman-echo.com/post",
            body=data)

        for platform in self.platforms:
            with self.subTest(msg=platform["name"]):
                ingestor = http.HttpIngestor(http_request, platform["stream_writer"])
                ingestor.ingest()
                result = platform["stream_reader"].read_str()
                result = json.loads(result)
                self.assertEqual(
                    result["data"],
                    data,
                    f"Expected to contain data: {data}: {result}")


if __name__ == '__main__':
    unittest.main()