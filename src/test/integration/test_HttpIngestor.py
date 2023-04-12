""" Unit tests """

import json
import os
import unittest
from requests.auth import HTTPBasicAuth
from xcputils.ingestion.file import FileIngestor
from xcputils.ingestion.http import HttpIngestor, HttpMethod, HttpRequest
from xcputils.streaming.aws import AwsS3StreamReader, AwsS3ConnectionSettings
from xcputils.streaming.az import AdfsStreamReader, AdfsConnectionSettings

class TestHttpIngestor(unittest.TestCase):
    """ Test xcputils.ingest.http """

    def setUp(self):
        self.aws_connection_settings = AwsS3ConnectionSettings(
            bucket=os.environ['AWS_S3_BUCKET'],
            file_path="tests3streamconnector/folder/test.txt"
            )

        self.adfs_connection_settings = AdfsConnectionSettings(
            container="testadfsstreamconnector",
            file_name="test.txt",
            directory="folder"
            )

        self.file_path = "./test.txt"


    def tearDown(self):
        os.remove(self.file_path)


    def test_get(self):
        """ Test xcputils.ingest.http.get """

        http_ingestor = HttpIngestor(http_request=HttpRequest(url="https://postman-echo.com/ip"))

        result = http_ingestor.write_to_string()
        self.assertTrue("ip" in result, f"key 'ip' not in: {result}")

        http_ingestor.write_to_file(self.file_path)
        result = FileIngestor(self.file_path).write_to_string()
        self.assertTrue("ip" in result, f"key 'ip' not in: {result}")


        http_ingestor.write_to_aws_s3(
            bucket=self.aws_connection_settings.bucket,
            file_path=self.aws_connection_settings.file_path
            )
        result = AwsS3StreamReader(self.aws_connection_settings).read_str()
        self.assertTrue("ip" in result, f"key 'ip' not in: {result}")

        http_ingestor.write_to_adfs(
            container=self.adfs_connection_settings.container,
            file_name=self.adfs_connection_settings.file_name,
            directory=self.adfs_connection_settings.directory
            )
        result = AdfsStreamReader(self.adfs_connection_settings).read_str()
        self.assertTrue("ip" in result, f"key 'ip' not in: {result}")


    def test_get_html(self):
        """ Test xcputils.ingest.http.get HTML """

        http_ingestor = HttpIngestor(http_request=HttpRequest(url="https://postman-echo.com"))

        result = http_ingestor.write_to_string()
        self.assertEqual(result[0:15], "<!DOCTYPE html>")

        http_ingestor.write_to_file(self.file_path)
        result = FileIngestor(self.file_path).write_to_string()
        self.assertEqual(result[0:15], "<!DOCTYPE html>")


        http_ingestor.write_to_aws_s3(
            bucket=self.aws_connection_settings.bucket,
            file_path=self.aws_connection_settings.file_path
            )
        result = AwsS3StreamReader(self.aws_connection_settings).read_str()
        self.assertEqual(result[0:15], "<!DOCTYPE html>")

        http_ingestor.write_to_adfs(
            container=self.adfs_connection_settings.container,
            file_name=self.adfs_connection_settings.file_name,
            directory=self.adfs_connection_settings.directory
            )
        result = AdfsStreamReader(self.adfs_connection_settings).read_str()
        self.assertEqual(result[0:15], "<!DOCTYPE html>")



    def test_auth(self):
        """ Test xcputils.ingest.http.get authentication """

        http_ingestor = HttpIngestor(
            http_request=HttpRequest(
                url="https://postman-echo.com/basic-auth",
                auth=HTTPBasicAuth('postman', 'password')
                ))

        result = http_ingestor.write_to_string()
        result = json.loads(result)
        self.assertEqual(result["authenticated"], True, result)

        http_ingestor.write_to_file(self.file_path)
        result = FileIngestor(self.file_path).write_to_string()
        result = json.loads(result)
        self.assertEqual(result["authenticated"], True, result)


        http_ingestor.write_to_aws_s3(
            bucket=self.aws_connection_settings.bucket,
            file_path=self.aws_connection_settings.file_path
            )
        result = AwsS3StreamReader(self.aws_connection_settings).read_str()
        result = json.loads(result)
        self.assertEqual(result["authenticated"], True, result)

        http_ingestor.write_to_adfs(
            container=self.adfs_connection_settings.container,
            file_name=self.adfs_connection_settings.file_name,
            directory=self.adfs_connection_settings.directory
            )
        result = AdfsStreamReader(self.adfs_connection_settings).read_str()
        result = json.loads(result)
        self.assertEqual(result["authenticated"], True, result)


    def test_post(self):
        """ Test xcputils.ingest.http.post """

        data = {"test_key": "test_value"}

        http_ingestor = HttpIngestor(
            http_request=HttpRequest(
                url="https://postman-echo.com/post",
                method=HttpMethod.POST,
                body=data,
                ))

        result = http_ingestor.write_to_string()
        result = json.loads(result)
        self.assertEqual(result["data"], data)

        http_ingestor.write_to_file(self.file_path)
        result = FileIngestor(self.file_path).write_to_string()
        result = json.loads(result)
        self.assertEqual(result["data"], data)


        http_ingestor.write_to_aws_s3(
            bucket=self.aws_connection_settings.bucket,
            file_path=self.aws_connection_settings.file_path
            )
        result = AwsS3StreamReader(self.aws_connection_settings).read_str()
        result = json.loads(result)
        self.assertEqual(result["data"], data)

        http_ingestor.write_to_adfs(
            container=self.adfs_connection_settings.container,
            file_name=self.adfs_connection_settings.file_name,
            directory=self.adfs_connection_settings.directory
            )
        result = AdfsStreamReader(self.adfs_connection_settings).read_str()
        result = json.loads(result)
        self.assertEqual(result["data"], data)


if __name__ == '__main__':
    unittest.main()
