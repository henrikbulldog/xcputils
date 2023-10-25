""" Unit tests """

from datetime import datetime
import os
import unittest
from xcputils.checkpoints.file import FileCheckpoint


class TestFileCheckpoint(unittest.TestCase):
    """ Test xcputils.checkpoints.az.FileCheckpoint """


    def test_file_checkpoint(self):
        """ xcputils.checkpoints.az.FileCheckpoint """

        if not os.path.isdir("data"):
            os.mkdir("data")

        checkpoint = FileCheckpoint(
            name="dev.test.test_file_checkpoint",
            directory="data",
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
