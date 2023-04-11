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

        adfs_connection_settings = AdfsConnectionSettings(
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
                "stream_writer": AdfsStreamWriter(adfs_connection_settings),
                "stream_reader": AdfsStreamReader(adfs_connection_settings),
            },
        ]


    def test_get(self):
        """ Test xcputils.ingest.http.get """

        for platform in self.platforms:

            with self.subTest(msg=platform["name"]):

                http.HttpIngestor() \
                    .read(url="https://postman-echo.com/ip") \
                    .with_stream_writer(platform["stream_writer"]) \
                    .ingest()

                result = platform["stream_reader"].read_str()

                self.assertTrue("ip" in result, f"key 'ip' not in: {result}")


    def test_get_html(self):
        """ Test xcputils.ingest.http.get HTML """

        for platform in self.platforms:

            with self.subTest(msg=platform["name"]):

                http.HttpIngestor() \
                    .read(url="https://postman-echo.com") \
                    .with_stream_writer(platform["stream_writer"]) \
                    .ingest()

                result = platform["stream_reader"].read_str()

                self.assertEqual(result[0:15], "<!DOCTYPE html>")


    def test_auth(self):
        """ Test xcputils.ingest.http.get authentication """

        for platform in self.platforms:

            with self.subTest(msg=platform["name"]):
                http.HttpIngestor() \
                    .read(
                        url="https://postman-echo.com/basic-auth",
                        auth=HTTPBasicAuth('postman', 'password'),
                    ) \
                    .with_stream_writer(platform["stream_writer"]) \
                    .ingest()

                result = platform["stream_reader"].read_str()

                result = json.loads(result)

                self.assertEqual(result["authenticated"], True, result)


    def test_post(self):
        """ Test xcputils.ingest.http.post """

        data = {"test_key": "test_value"}

        for platform in self.platforms:
            with self.subTest(msg=platform["name"]):
                http.HttpIngestor() \
                    .read(
                        url="https://postman-echo.com/post",
                        method=http.HttpMethod.POST,
                        body=data,
                    ) \
                    .with_stream_writer(platform["stream_writer"]) \
                    .ingest()

                result = platform["stream_reader"].read_str()

                result = json.loads(result)

                self.assertEqual(
                    result["data"],
                    data,
                    f"Expected to contain data: {data}: {result}")


if __name__ == '__main__':
    unittest.main()
