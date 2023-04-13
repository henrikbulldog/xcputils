""" Main entry point """

from xcputils import XCPUtils

xcputils = XCPUtils()

result = xcputils.read_from_http(url="https://postman-echo.com/ip") \
    .write_to_string()

print(result)


data = "testing 1-2-3"

xcputils.read_from_string(data) \
    .write_to_aws_s3(
        bucket="hethcotest1",
        file_path=f"test/testing.txt",
    )

result = xcputils.read_from_aws_s3(
    bucket="hethcotest1",
    file_path=f"test/testing.txt",
    ) \
    .write_to_string()

print(result)
