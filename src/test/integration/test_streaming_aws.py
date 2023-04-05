""" Unit tests """

import datetime
import os
import unittest

from xcputils.streaming.aws import AwsS3StreamReader, AwsS3StreamWriter, AwsS3ConnectionSettings


class TestS3StreamConnector(unittest.TestCase):
    """ Test xcputils.streaming.aws.S3StreamConnector """


    def test_write_read(self):
        """ xcputils.streaming.aws.S3StreamConnector write and read """

        connection_settings = AwsS3ConnectionSettings(
            bucket=os.environ['AWS_S3_BUCKET'],
            file_path="tests3streamconnector/folder/test.txt")

        writer = AwsS3StreamWriter(connection_settings)

        payload = f"Testing.\n123.\næøåÆØÅ\n{datetime.datetime.now()}"

        writer.write_str(payload)

        reader = AwsS3StreamReader(connection_settings)

        actual_payload = reader.read_str()

        self.assertEqual(actual_payload, payload)


if __name__ == '__main__':
    unittest.main()
