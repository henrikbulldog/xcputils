""" ADFS Checkpoint """

from xcputils.checkpoints import Checkpoint
from xcputils.ingestion.az import AdfsConnectionSettings, AdfsIngestor
from xcputils.ingestion.string import StringIngestor
from xcputils.streaming.az import AdfsStreamWriter


class AdfsCheckpoint(Checkpoint):
    """ ADFS Checkpoint """

    def __init__(
        self,
        name: str,
        container: str,
        directory: str,
        storage_account_name: str = None,
        tenant_id: str = None,
        client_id: str = None,
        client_secret: str = None,
        ):
        """ Constructor """
        self.adfs_connection_settings = AdfsConnectionSettings(
            container=container,
            file_name=f"{name}.json",
            directory=directory,
            storage_account_name=storage_account_name,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            )

    def get(self, default: str = None):
        """ Get checkpoint """
        try:
            checkpoint = AdfsIngestor(self.adfs_connection_settings).write_to_string()
        except:
            checkpoint = ""
        
        if checkpoint == "":
            checkpoint = default

        return checkpoint
    

    def set(self, value: str):
        """ Set checkpoint """
        StringIngestor(value, AdfsStreamWriter(self.adfs_connection_settings)).ingest()
