""" Test File Ingestion """

import datetime
from io import BytesIO
import os
import unittest
from xcputils.ingestion.file import FileIngestor

from xcputils.streaming.file import FileStreamWriter



class TestStringStreamWriter(unittest.TestCase):
    """ Test FileStreamWriter """

    def setUp(self):
        self.file_path = "./test_file.txt"


    def tearDown(self):
        os.remove(self.file_path)


    def test_write(self):
        """ Test write """

        payload = f"Testing.\n123.\næøåÆØÅ\n{datetime.datetime.now()}"
        writer = FileStreamWriter(self.file_path)

        with BytesIO() as stream:
            stream.write(payload.encode('utf-8'))
            stream.seek(0)
            writer.write(stream)

        result = FileIngestor(self.file_path).write_to_string()

        self.assertEqual(result, payload)
