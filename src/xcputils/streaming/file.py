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

    def get_file_path(self) -> str:
        """ Get filename """

        return self.file_path


    def set_file_path(self, file_path: str):
        """ Set filename """

        self.file_path = file_path
