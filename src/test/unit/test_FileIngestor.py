""" Test File Ingestion """

import os
import unittest

from xcputils.ingestion.file import FileIngestor


class TestFileIngestor(unittest.TestCase):
    """ Test FileIngestor """

    def setUp(self):
        self.file_content = "Hello, world! This is a test file."
        self.file_path = "./test_file.txt"
        with open(self.file_path, "w", encoding="utf-8") as file:
            file.write(self.file_content)


    def tearDown(self):
        os.remove(self.file_path)


    def test_ingest(self):
        """ Test ingest """

        result = FileIngestor(self.file_path).write_to_string()
        self.assertEqual(result, self.file_content)


    def test_init(self):
        """ Test init """

        ingestor = FileIngestor(self.file_path)
        self.assertEqual(ingestor.file_path, self.file_path)
        self.assertEqual(ingestor.stream_writer, None)
