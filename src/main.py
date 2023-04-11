""" Main entry point """

from xcputils import XCPUtils

xcputils = XCPUtils()

result = xcputils.http.read(url="https://postman-echo.com/ip") \
    .write_to_string()

print(result)
