""" Unit tests """

import datetime
import os
import unittest
from xcputils.ingestion.aws import AwsS3Ingestor
from xcputils.ingestion.string import StringIngestor
from xcputils.streaming.aws import AwsS3ConnectionSettings


class TestAwsS3Ingestor(unittest.TestCase):
    """ Test xcputils.ingestion.aws.AwsS3Ingestor """


    def test_ingest(self):
        """ xcputils.streaming.aws.AwsS3Ingestor """

        bucket = os.environ['AWS_S3_BUCKET']
        file_path = "tests3ingestor/test.txt"
        payload = f"Testing.\n123.\næøåÆØÅ\n{datetime.datetime.now()}"


        StringIngestor(data=payload) \
            .write_to_aws_s3(bucket, file_path)

        result = AwsS3Ingestor(AwsS3ConnectionSettings(bucket, file_path)).write_to_string()

        self.assertEqual(result, payload)


if __name__ == '__main__':
    unittest.main()
