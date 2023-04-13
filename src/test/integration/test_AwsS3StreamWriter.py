""" Unit tests """

import datetime
from io import BytesIO
import os
import unittest

from xcputils.streaming.aws import AwsS3StreamReader, AwsS3StreamWriter, AwsS3ConnectionSettings


class TestS3StreamWriter(unittest.TestCase):
    """ Test xcputils.streaming.aws.AwsS3StreamWriter """


    def test_write_read(self):
        """ xcputils.streaming.aws.S3StreamConnector write and read """

        connection_settings = AwsS3ConnectionSettings(
            bucket=os.environ['AWS_S3_BUCKET'],
            file_path="tests3streamwriter/test.txt")

        writer = AwsS3StreamWriter(connection_settings)

        payload = f"Testing.\n123.\næøåÆØÅ\n{datetime.datetime.now()}"

        with BytesIO() as stream:
            stream.write(payload.encode('utf-8'))
            stream.seek(0)
            writer.write(stream)

        reader = AwsS3StreamReader(connection_settings)

        actual_payload = reader.read_str()

        self.assertEqual(actual_payload, payload)


if __name__ == '__main__':
    unittest.main()
