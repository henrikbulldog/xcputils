""" File stream writer """

from xcputils.streaming import StreamWriter


class FileStreamWriter(StreamWriter):
    """ File stream writer """

    def __init__(self, file_path: str):
        super().__init__()
        self.file_path = file_path


    def write(self, input_stream):
        """ Write stream to file """

        with open(file=self.file_path, mode="wb") as file_stream:
            file_stream.write(input_stream.read())
