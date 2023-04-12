""" String stream writer """

from xcputils.streaming import StreamWriter


class StringStreamWriter(StreamWriter):
    """ String stream writer """


    def __init__(self):
        super().__init__()
        self.value = ""


    def write(self, input_stream):
        """ Write to stream """

        content = input_stream.read()
        if isinstance(content, bytes):
            content = content.decode('utf-8')
        self.value = content
