""" Unit tests """

import unittest
import datetime

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

        writer.write_str(payload)

        reader = AdfsStreamReader(connection_settings)
        actual_payload = reader.read_str()

        self.assertEqual(actual_payload, payload)


if __name__ == '__main__':
    unittest.main()
