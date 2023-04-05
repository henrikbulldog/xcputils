""" Unit tests """

import json
import os
import unittest
from xcputils.ingestion import http
from xcputils.streaming.aws import S3StreamConnector


class TestPaginatedHttpIngestor(unittest.TestCase):
    """ Test xcputils.ingest.http """


    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)

    def test_get(self):
        """ Test xcputils.ingest.http.get """

        stream_connector = S3StreamConnector(
            container=os.environ['AWS_S3_BUCKET'],
            file_name="",
            directory="testpaginatedhttpingestor/eds/co2emis"
            )

        request = http.HttpRequest(
            url="https://api.energidataservice.dk/dataset/CO2Emis",
            params={"start": "2022-01-01T00:00", "end": "2022-01-02T00:00"})

        handler = http.PaginationHandler(page_size=100, data_property="records")

        ingestor = http.PaginatedHttpIngestor(
            request=request,
            stream_connector=stream_connector,
            handler=handler)

        ingestor.ingest()

        stream_connector.file_name = "page-1.json"
        page_1 = json.loads(stream_connector.read_str())
        self.assertTrue(len(page_1["records"]) == 100)


if __name__ == "__main__":
    unittest.main()
