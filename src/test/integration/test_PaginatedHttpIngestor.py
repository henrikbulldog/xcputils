""" Unit tests """

import json
import os
import unittest
from xcputils.ingestion.http import HttpIngestor, HttpRequest
from xcputils.streaming.aws import AwsS3ConnectionSettings, AwsS3StreamReader


class TestPaginatedHttpIngestor(unittest.TestCase):
    """ Test xcputils.ingest.http """


    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)


    def test_get(self):
        """ Test xcputils.ingest.http.get """

        HttpIngestor(
            http_request=HttpRequest(
                url="https://api.energidataservice.dk/dataset/CO2Emis",
                params={"start": "2022-01-01T00:00", "end": "2022-01-02T00:00"})
            ) \
            .with_pagination(
                page_size=100,
                data_property="records",
            ) \
            .write_to_aws_s3(
                bucket=os.environ['AWS_S3_BUCKET'],
                file_path="testpaginatedhttpingestor/eds/co2emis/co2emis.json"
                )

        stream_reader = AwsS3StreamReader(
            AwsS3ConnectionSettings(
                bucket=os.environ['AWS_S3_BUCKET'],
                file_path="testpaginatedhttpingestor/eds/co2emis/co2emis.1.json"
                )
            )

        page_1 = json.loads(stream_reader.read_str())
        self.assertTrue(len(page_1["records"]) == 100)

        stream_reader.connection_settings.file_path = \
            "testpaginatedhttpingestor/eds/co2emis/co2emis.2.json"
        page_6 = json.loads(stream_reader.read_str())
        self.assertTrue(len(page_6["records"]) == 100)


if __name__ == "__main__":
    unittest.main()
