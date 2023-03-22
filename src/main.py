""" example taken from: 
    https://docs.databricks.com/dev-tools/databricks-connect.html#access-dbutils """

import os

from xcputils import XCPUtils

xcputils = XCPUtils()


bucket_name = os.getenv("AWS_S3_BUCKET", "My-bucket")
folder = "bronze/eds"
file_name = "co2emis.json"

connector = xcputils.create_string_stream_connector()

xcputils.http.get(url="https://postman-echo.com/ip",
    connector=connector)

print(connector.read_str())
