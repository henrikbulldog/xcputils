""" Unit tests """

from datetime import datetime
import os
import unittest
from xcputils.checkpoints.aws import AwsS3Checkpoint


class TestAwsS3Checkpoint(unittest.TestCase):
    """ Test xcputils.checkpoints.az.AwsS3Checkpoint """


    def test_adfs_checkpoint(self):
        """ xcputils.checkpoints.az.AwsS3Checkpoint """

        checkpoint = AwsS3Checkpoint(
            name="dev.test.test_awss3_checkpoint",
            bucket=os.environ['AWS_S3_BUCKET'],
            directory="testawss3checkpoint",
            )

        checkpoint.reset()
        expected = "default"
        actual = checkpoint.get(default=expected)
        self.assertEqual(actual, expected)

        expected = str(datetime.now())
        checkpoint.set(expected)
        actual = checkpoint.get()
        self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
