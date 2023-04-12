""" Main entry point """

from xcputils import XCPUtils

xcputils = XCPUtils()

result = xcputils.read_from_http(url="https://postman-echo.com/ip") \
    .write_to_string()

print(result)
