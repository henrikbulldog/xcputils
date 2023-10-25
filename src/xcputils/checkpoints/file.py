""" File Checkpoint """

from xcputils.checkpoints import Checkpoint
from xcputils.ingestion.file import FileIngestor
from xcputils.ingestion.string import StringIngestor
from xcputils.streaming.file import FileStreamWriter


class FileCheckpoint(Checkpoint):
    """ File Checkpoint """

    def __init__(
        self,
        name: str,
        directory: str,
        ):
        """ Constructor """
        self.file_path=f"{directory}/{name}.json"

    def get(self, default: str = None) -> str:
        """ Get checkpoint """
        try:
            checkpoint = FileIngestor(self.file_path).write_to_string()
        except:
            checkpoint = ""
        
        if checkpoint == "":
            checkpoint = default

        return checkpoint

    def set(self, value: str):
        """ Set checkpoint """
        StringIngestor(value, FileStreamWriter(self.file_path)).ingest()
