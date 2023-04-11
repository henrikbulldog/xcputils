""" Package xcputils """

from xcputils.ingestion.http import HttpIngestor, HttpMethod, PaginatedHttpIngestor


class XCPUtils():
    """ Utilities for copying data """


    def from_http(
        self,
        url: str,
        method: HttpMethod = HttpMethod.GET,
        params: dict = None,
        body: dict = None,
        headers: dict = None,
        auth = None,
        ):
        """ Ingest from HTTP """

        return HttpIngestor(
            url=url,
            method=method,
            params=params,
            body=body,
            headers=headers,
            auth=auth)


    def from_http_paginated(
        self,
        url: str,
        method: HttpMethod = HttpMethod.GET,
        params: dict = None,
        body: dict = None,
        headers: dict = None,
        auth = None,
        page_size: int=1000,
        page_size_param: str = "limit",
        data_property: str = "data",
        max_pages: int = 1000):
        """ Ingest from HTTP """

        return PaginatedHttpIngestor(
            url=url,
            method=method,
            params=params,
            body=body,
            headers=headers,
            auth=auth,
            page_size=page_size,
            page_size_param=page_size_param,
            data_property=data_property,
            max_pages=max_pages)
