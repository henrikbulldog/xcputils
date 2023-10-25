""" File ingestion """

from ftplib import FTP
from io import BytesIO
from xcputils.ingestion import Ingestor
from xcputils.streaming import StreamWriter


class FtpIngestor(Ingestor):
    """ Ingest from FTP """

    def __init__(
        self,
        url: str,
        folder: str = None,
        file_name: str = None,
        user: str = None,
        password: str = None,
        stream_writer: StreamWriter = None,
    ):

        super().__init__(stream_writer)

        self.url = url
        self.folder = folder
        self.file_name = file_name
        self.user = user
        self.password = password


    def __is_dir(self, ftp, filename):
        try:
            ftp.size(filename)
            return False
        except:
            return True


    def ingest(self):
        """ Ingest """

        ftp = FTP(host=self.url, user=self.user, passwd=self.password)

        if self.folder:
            ftp.cwd(self.folder)

        for file_name in ftp.nlst():
            if not self.__is_dir(ftp, file_name) \
                and (not self.file_name or self.file_name.lower() == file_name.lower()):
                hold_filename = self.stream_writer.get_file_path()
                self.stream_writer.set_file_path(
                    file_name if not hold_filename else f"{hold_filename}/{file_name}")
                ftp.retrbinary(f"RETR {file_name}", self.__write)
                self.stream_writer.set_file_path(hold_filename)

        ftp.quit()

    def __write(self, bytes):
        with BytesIO() as stream:
            stream.write(bytes)
            stream.seek(0)
            self.stream_writer.write(stream)
