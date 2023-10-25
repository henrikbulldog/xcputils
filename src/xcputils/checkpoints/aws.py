""" ADFS Checkpoint """

from xcputils.checkpoints import Checkpoint
from xcputils.ingestion.aws import AwsS3Ingestor
from xcputils.ingestion.string import StringIngestor
from xcputils.streaming.aws import AwsS3ConnectionSettings, AwsS3StreamWriter


class AwsS3Checkpoint(Checkpoint):
    """ ADFS Checkpoint """

    def __init__(
        self,
        name: str,
        bucket: str,
        directory: str,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        aws_session_token: str = None,
        aws_region_name: str = None,
        ):
        """ Constructor """
        self.awss3_connection_settings = AwsS3ConnectionSettings(
            bucket=bucket,
            file_path=f"{directory}/{name}.json",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            aws_region_name=aws_region_name,
            )

    def get(self, default: str = None):
        """ Get checkpoint """
        try:
            checkpoint = AwsS3Ingestor(self.awss3_connection_settings).write_to_string()
        except:
            checkpoint = ""
        
        if checkpoint == "":
            checkpoint = default

        return checkpoint

    def set(self, value: str):
        """ Set checkpoint """
        StringIngestor(value, AwsS3StreamWriter(self.awss3_connection_settings)).ingest()
