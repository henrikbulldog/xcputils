""" Main entry point """

from xcputils import XCPUtils

xcputils = XCPUtils()

result = xcputils.from_http(url="https://postman-echo.com/ip") \
    .to_string()

print(result)
