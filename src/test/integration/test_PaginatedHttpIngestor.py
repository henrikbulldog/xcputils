""" Unit tests """

import json
import os
import unittest
from xcputils.ingestion import http
from xcputils.streaming.aws import AwsS3StreamWriter, AwsS3ConnectionSettings, AwsS3StreamReader


class TestPaginatedHttpIngestor(unittest.TestCase):
    """ Test xcputils.ingest.http """


    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)


    def __get_connection_settings(self, page_number):
        return AwsS3ConnectionSettings(
            bucket=os.environ['AWS_S3_BUCKET'],
            file_path=f"testpaginatedhttpingestor/eds/co2emis/page-{page_number}.json"
            )


    def test_get(self):
        """ Test xcputils.ingest.http.get """

        http.PaginatedHttpIngestor(
            url="https://api.energidataservice.dk/dataset/CO2Emis") \
            .with_page_size(100) \
            .with_params({"start": "2022-01-01T00:00", "end": "2022-01-02T00:00"}) \
            .to_aws_s3(
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


if __name__ == "__main__":
    unittest.main()
