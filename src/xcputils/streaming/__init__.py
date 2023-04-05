""" Connectors to read and write streams """

from io import BytesIO
import tempfile
from typing import Any


class StreamReader():
    """ Stream reader base class """

    def read(self, output_stream: Any):
        """ Read from stream """


    def read_str(self) -> str:
        """ Read from stream to a string """

        with tempfile.TemporaryFile() as data:
            self.read(data)
            data.seek(0)
            return data.read().decode('utf-8')


class StreamWriter():
    """ Stream writer base class """

    def write(self, input_stream: Any):
        """ Write stream """


    def write_str(self, data: str):
        """ Write string """

        with BytesIO() as stream:
            stream.write(data.encode('utf-8'))
            stream.seek(0)
            self.write(stream)
