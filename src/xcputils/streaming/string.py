""" String writer """

from typing import Any
from xcputils.streaming import StreamConnector


class StringStreamConnector(StreamConnector):
    """ String writer """

    LOG = []


    def __init__(self):
        super().__init__()
        self.value = ""


    def read(self, output_stream: Any):
        """ Read from stream """
        output_stream.write(bytes(self.value, 'utf-8'))


    def write(self, input_stream):
        """ Write to stream """

        content = input_stream.read()
        if isinstance(content, bytes):
            content = content.decode('utf-8')
        self.value = content
        StringStreamConnector.LOG.append(self.value)
