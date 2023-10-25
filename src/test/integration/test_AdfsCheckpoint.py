""" Unit tests """

from datetime import datetime
import unittest
from xcputils.checkpoints.az import AdfsCheckpoint


class TestAdfsCheckpoint(unittest.TestCase):
    """ Test xcputils.checkpoints.az.AdfsCheckpoint """


    def test_adfs_checkpoint(self):
        """ xcputils.checkpoints.az.AdfsCheckpoint """

        checkpoint = AdfsCheckpoint(
            name="dev.test.test_adfs_checkpoint",
            container="testxcputils",
            directory="testadfscheckpoint",
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
