""" Package xcputils """

from xcputils.ingestion.http import HttpIngestor, HttpMethod, PaginatedHttpIngestor, HttpRequest, PaginationHandler
from xcputils.streaming import StreamWriter
from xcputils.streaming.aws import AwsS3StreamReader, AwsS3StreamWriter, AwsS3ConnectionSettings
from xcputils.streaming.az import AdfsStreamReader, AdfsStreamWriter, AdfsConnectionSettings
from xcputils.streaming.string import StringStreamWriter


class XCPUtils():
    """ Utilities for copying data """

    def create_http_request(
        self,
        url: str,
        method: HttpMethod = HttpMethod.GET,
        params: dict = None,
        body: dict = None,
        headers: dict = None,
        auth = None) -> HttpRequest:
        """ Create a HTTTP ingestor """

        return HttpRequest(
            url=url,
            method=method,
            params=params,
            body=body,
            headers=headers,
            auth=auth
        )


    def create_http_ingestor(self, http_request: HttpRequest, stream_writer: StreamWriter) -> HttpIngestor:
        """ Create a HTTTP ingestor """

        return HttpIngestor(http_request=http_request, stream_writer=stream_writer)


    def create_paginated_http_ingestor(
        self,
        http_request: HttpRequest,
        stream_writer: StreamWriter,
        pagination_handler: PaginationHandler,
        file_pattern: str = "page-{page_number}.json"):
        """ Create a paginated HTTTP ingestor """

        return PaginatedHttpIngestor(
            http_request=http_request,
            stream_writer=stream_writer,
            pagination_handler=pagination_handler,
            file_pattern=file_pattern)

    def create_string_stream_writer(self) -> StringStreamWriter:
        """ Create an AWS S3 stream writer """

        return StringStreamWriter()


    def create_aws_s3_stream_reader(
        self,
        connection_settings: AwsS3ConnectionSettings
        ) -> AwsS3StreamReader:
        """ Create an AWS S3 stream reader """

        return AwsS3StreamReader(connection_settings)


    def create_aws_s3_stream_writer(
        self,
        connection_settings: AwsS3ConnectionSettings
        ) -> AwsS3StreamWriter:
        """ Create an AWS S3 stream writer """

        return AwsS3StreamWriter(connection_settings)


    def create_adfs_stream_reader(
        self,
        connection_settings: AdfsConnectionSettings
        ) -> AdfsStreamReader:
        """ Create an Azure Data File System stream reader """

        return AdfsStreamReader(connection_settings)


    def create_adfs_stream_writer(
        self,
        connection_settings: AdfsConnectionSettings
        ) -> AdfsStreamWriter:
        """ Create an Azure Data File System stream writer """

        return AdfsStreamWriter(connection_settings)
