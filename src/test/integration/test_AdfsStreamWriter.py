""" Unit tests """

from io import BytesIO
import unittest
import datetime
from xcputils.ingestion.string import StringIngestor

from xcputils.streaming.az import AdfsStreamWriter, AdfsConnectionSettings, AdfsStreamReader


class TestAdfsStreamWriter(unittest.TestCase):
    """ Test  """


    def test_write_read(self):
        """ Test AdfsStreamConnector.read """

        connection_settings = AdfsConnectionSettings(
            container="testadfsstreamconnector",
            directory="folder",
            file_name="test.txt")

        writer = AdfsStreamWriter(connection_settings)

        payload = f"Testing.\n123.\næøåÆØÅ\n{datetime.datetime.now()}"

        with BytesIO() as stream:
            stream.write(payload.encode('utf-8'))
            stream.seek(0)
            writer.write(stream)

        reader = AdfsStreamReader(connection_settings)
        actual_payload = reader.read_str()

        self.assertEqual(actual_payload, payload)


if __name__ == '__main__':
    unittest.main()
