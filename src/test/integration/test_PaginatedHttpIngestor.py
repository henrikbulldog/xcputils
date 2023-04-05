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

        http_request = http.HttpRequest(
            url="https://api.energidataservice.dk/dataset/CO2Emis",
            params={"start": "2022-01-01T00:00", "end": "2022-01-02T00:00"})

        pagination_handler = http.PaginationHandler(page_size=100, data_property="records")

        http_ingestor = http.PaginatedHttpIngestor(
            http_request=http_request,
            pagination_handler=pagination_handler,
            get_stream_writer=lambda page_number: AwsS3StreamWriter(self.__get_connection_settings(page_number)))

        http_ingestor.ingest()

        stream_reader = AwsS3StreamReader(self.__get_connection_settings(1))
        page_1 = json.loads(stream_reader.read_str())
        self.assertTrue(len(page_1["records"]) == 100)


if __name__ == "__main__":
    unittest.main()
