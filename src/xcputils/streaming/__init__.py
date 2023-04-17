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


    def get_file_path(self) -> str:
        """ Get filename """
        return ""


    def set_file_path(self, file_path: str):
        """ Set filename """


    def write(self, input_stream: Any):
        """ Write stream """
