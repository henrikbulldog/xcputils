""" Unit tests """

import unittest
import datetime
from io import BytesIO

from xcputils.streaming.az import AdfsStreamConnector


class TestAz(unittest.TestCase):
    """ Test  """


    def test_write_read(self):
        """ Test AdfsStreamConnector.read """

        connector = AdfsStreamConnector(
            container="testadfsstreamconnector",
            directory="folder",
            file_name="test.txt")

        payload = f"Testing.\n123.\næøåÆØÅ\n{datetime.datetime.now()}"

        with BytesIO() as stream:
            stream.write(payload.encode('utf-8'))
            stream.seek(0)
            connector.write(stream)

        actual_payload = connector.read_str()

        self.assertEqual(actual_payload, payload)


if __name__ == '__main__':
    unittest.main()