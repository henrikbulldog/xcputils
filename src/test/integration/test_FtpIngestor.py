""" Unit tests """

import os
import unittest
from xcputils.ingestion.ftp import FtpIngestor
from xcputils.streaming.aws import AwsS3ConnectionSettings, AwsS3StreamReader
from xcputils.streaming.az import AdfsConnectionSettings, AdfsStreamReader


class TestFtpIngestor(unittest.TestCase):
    """ Test xcputils.ingestion.ftp.FtpIngestor """

    def test_ingest(self):
        """ xcputils.streaming.ftp.FtpIngestor """

        download_path = "./data/dlptest"
        os.makedirs(name=download_path, exist_ok=True)

        ingestor = FtpIngestor(
            url="test.rebex.net",
            folder=None,
            file_name=None,
            user="demo",
            password="password",
            )

        ingestor.write_to_file(download_path)

        files = os.listdir(download_path)
        file_name = files[0]
        os.system(f"ls -l {download_path}")
        self.assertTrue(len(files) > 0)
        os.system(f"rm -r {download_path}")
        os.makedirs(name=download_path, exist_ok=True)

        ingestor.file_name = file_name
        ingestor.write_to_file(download_path)
        files = os.listdir(download_path)
        os.system(f"ls -l {download_path}")
        self.assertTrue(len(files) == 1)
        os.system(f"rm -r {download_path}")

        result = ingestor.write_to_string()
        self.assertTrue(len(result) > 0)

        ingestor.write_to_aws_s3(
            bucket=os.environ['AWS_S3_BUCKET'],
            file_path="testfptingetor/temp"
            )
        result = AwsS3StreamReader(
            connection_settings=AwsS3ConnectionSettings(
                bucket=os.environ['AWS_S3_BUCKET'],
                file_path=f"testfptingetor/temp/{file_name}"
            )
        ).read_str()
        self.assertTrue(len(result) > 0)

        ingestor.write_to_adfs(
            container="testfptingetor",
            file_name="",
            directory="temp"
            )
        result = AdfsStreamReader(
            connection_settings=AdfsConnectionSettings(
                container="testfptingetor",
                file_name=file_name,
                directory="temp"
            )
        ).read_str()
        self.assertTrue(len(result) > 0)


if __name__ == '__main__':
    unittest.main()
