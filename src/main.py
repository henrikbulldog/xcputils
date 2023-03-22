""" example taken from: 
    https://docs.databricks.com/dev-tools/databricks-connect.html#access-dbutils """

import os
from xcputils import xcputils

spark = SparkSession.builder.getOrCreate()
xcputils = xcputils()


bucket_name = os.getenv("AWS_S3_BUCKET", "My-bucket")
folder = "bronze/eds"
file_name = "co2emis.json"

connector = xcputils.create_aws_s3_stream_connector(
    bucket_name=bucket_name,
    file_path=f"{folder}/{file_name}")

xcputils.ingestion.http.get(url="https://api.energidataservice.dk/dataset/CO2Emis",
    connector=connector,
    params={"start": "2022-01-01T00:00", "end": "2022-01-01T01:00"})

s3_path = f"s3://{bucket_name}/{folder}"

