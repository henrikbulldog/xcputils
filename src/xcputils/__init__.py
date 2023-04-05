""" Package xcputils """

from typing import Callable
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

    def create_pagination_handler(
        self,
        page_size: int,
        page_size_param: str = "limit",
        data_property: str = "data",
        max_pages: int = 1000) -> PaginationHandler:
        """ Create pagination handler """

        return PaginationHandler(
            page_size=page_size,
            page_size_param=page_size_param,
            data_property=data_property,
            max_pages=max_pages
        )


    def create_paginated_http_ingestor(
        self,
        http_request: HttpRequest,
        pagination_handler: PaginationHandler,
        get_stream_writer: Callable[[int], StreamWriter]
        ) -> PaginatedHttpIngestor:
        """ Create a paginated HTTTP ingestor """

        return PaginatedHttpIngestor(
            http_request=http_request,
            pagination_handler=pagination_handler,
            get_stream_writer=get_stream_writer)

    def create_string_stream_writer(self) -> StringStreamWriter:
        """ Create an AWS S3 stream writer """

        return StringStreamWriter()


    def create_aws_s3_connection_settings(
        self,
        bucket: str, 
        file_path: str,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        aws_session_token: str = None,
        aws_region_name: str = None
        ) -> AwsS3ConnectionSettings:
        """ Create an AWS S3 connection settings """

        return AwsS3ConnectionSettings(
            bucket=bucket,
            file_path=file_path,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            aws_region_name=aws_region_name
        )


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


    def create_adfs_connection_settings(
        self,
        container: str,
        file_name: str,
        directory: str,
        storage_account_name: str = None
        ) -> AdfsConnectionSettings:
        """ Create an Azure Data File System connection settings """

        return AdfsConnectionSettings(
            container=container,
            file_name=file_name,
            directory=directory,
            storage_account_name=storage_account_name
            )


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
