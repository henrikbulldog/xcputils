""" Main entry point """

from xcputils import XCPUtils

xcputils = XCPUtils()

stream_writer = xcputils.create_string_stream_writer()
http_request = xcputils.create_http_request(url="https://postman-echo.com/ip")
http_ingestor = xcputils.create_http_ingestor(http_request=http_request, stream_writer=stream_writer)

http_ingestor.ingest()

print(stream_writer.value)
