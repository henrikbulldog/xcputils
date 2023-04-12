""" Test File Ingestion """

import datetime
import unittest
from xcputils.ingestion.string import StringIngestor


class TestStringIngestor(unittest.TestCase):
    """ Test StringIngestor """

    def test_ingest(self):
        """ Test ingest """

        payload = f"Testing.\n123.\næøåÆØÅ\n{datetime.datetime.now()}"
        result = StringIngestor(payload).write_to_string()

        self.assertEqual(result, payload)
