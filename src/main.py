""" Main entry point """

from xcputils import XCPUtils

xcputils = XCPUtils()

stream_connector = xcputils.create_string_stream_connector()
request = xcputils.create_http_request(url="https://postman-echo.com/ip")
ingestor = xcputils.create_http_ingestor(request=request, stream_connector=stream_connector)

ingestor.ingest()

print(stream_connector.read_str())
