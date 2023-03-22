""" Unit tests """

import os
import unittest
import json
from requests.auth import HTTPBasicAuth
from xcputils.ingestion import http
from xcputils.streaming.aws import S3StreamConnector
from xcputils.streaming.az import AdfsStreamConnector
from xcputils.streaming.string import StringStreamConnector


class TestIngestHttp(unittest.TestCase):
    """ Test xcputils.ingest.http """


    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.connectors = [
            StringStreamConnector(),
            S3StreamConnector(
                bucket_name=os.environ['AWS_S3_BUCKET'],
                file_path="tests3streamconnector/folder/test.txt"
                ),
            AdfsStreamConnector(
                container="testadfsstreamconnector",
                directory="folder",
                file_name="test.txt"),
        ]


    def test_get(self):
        """ Test xcputils.ingest.http.get """

        for connector in self.connectors:
            with self.subTest():
                http.get(url="https://postman-echo.com/ip", connector=connector)
                result = connector.read_str()
                self.assertTrue("ip" in result, f"key 'ip' not in: {result}")


    def test_get_html(self):
        """ Test xcputils.ingest.http.get HTML """

        for connector in self.connectors:
            with self.subTest():
                http.get(url="https://postman-echo.com", connector=connector)
                result = connector.read_str()
                self.assertEqual(result[0:15], "<!DOCTYPE html>")


    def test_auth(self):
        """ Test xcputils.ingest.http.get authentication """

        for connector in self.connectors:
            with self.subTest():
                http.get(url="https://postman-echo.com/basic-auth", connector=connector, auth=HTTPBasicAuth('postman', 'password'))
                result = json.loads(connector.read_str())
                self.assertEqual(result["authenticated"], True, result)


    def test_post(self):
        """ Test xcputils.ingest.http.post """

        for connector in self.connectors:
            with self.subTest():
                expected = {"test_key": "test_value"}
                http.post(
                    url="https://postman-echo.com/post",
                    body={"test_key": "test_value"},
                    connector=connector)
                result = json.loads(connector.read_str())
                self.assertEqual(
                    result["data"],
                    expected,
                    f"Expected to contain data: {expected}: {result}")


    def test_get_paginated(self):
        """ Test xcputils.ingest.http.get """

        string_connector = StringStreamConnector()
        http.get(
            url="https://api.energidataservice.dk/dataset/CO2Emis",
            connector=string_connector,
            params={"start": "2022-01-01T00:00", "end": "2022-01-02T00:00"},
            )
        total = json.loads(string_connector.read_str())["total"]

        folder = "testhttpgetpaginated/bronze/CO2Emis"
        limit = 100

        http.get_paginated(
            url="https://api.energidataservice.dk/dataset/CO2Emis",
            create_connector=lambda offset: S3StreamConnector(
                bucket_name=os.environ['AWS_S3_BUCKET'],
                file_path=f"{folder}/{offset}.json"
                ),
            params={"start": "2022-01-01T00:00", "end": "2022-01-02T00:00"},
            limit=limit,
            maximum=total,
            )

        for offset in list(range(0, total, limit)):
            result = json.loads(
                S3StreamConnector(
                    bucket_name=os.environ['AWS_S3_BUCKET'],
                    file_path=f"{folder}/{offset}.json").read_str())

            self.assertEqual(result["total"], total)
            self.assertEqual(result["limit"], limit)
            self.assertEqual(result["dataset"], "CO2Emis")
            self.assertTrue(len(result["records"]) > 0)
            self.assertTrue(len(result["records"]) <= limit)


if __name__ == '__main__':
    unittest.main()
