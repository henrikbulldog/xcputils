""" Unit tests """

import datetime
import unittest
from xcputils.ingestion.az import AdfsIngestor
from xcputils.ingestion.string import StringIngestor
from xcputils.streaming.az import AdfsConnectionSettings


class TestAdfsIngestor(unittest.TestCase):
    """ Test xcputils.ingestion.az.AdfsIngestor """


    def test_ingest(self):
        """ xcputils.streaming.az.AdfsIngestor """

        container="testxcputils"
        directory="testadfsingestor"
        file_name="test.txt"
        payload = f"Testing.\n123.\næøåÆØÅ\n{datetime.datetime.now()}"


        StringIngestor(data=payload) \
            .write_to_adfs(
                container=container,
                directory=directory,
                file_name=file_name,
                )

        result = AdfsIngestor(
            AdfsConnectionSettings(
                container=container,
                directory=directory,
                file_name=file_name,
                )).write_to_string()

        self.assertEqual(result, payload)


if __name__ == '__main__':
    unittest.main()
