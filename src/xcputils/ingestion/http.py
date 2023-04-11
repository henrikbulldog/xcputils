""" HTTP Request callable """

import json
import os
from typing import Callable, ContextManager
from enum import Enum
import copy
import requests
from xcputils.ingestion import Ingestor

from xcputils.streaming import StreamWriter


class HttpMethod(Enum):
    """ HTTP method """

    GET = "GET"
    POST = "POST"


class HttpRequest():
    """ HTTP request parameters """

    def __init__(
        self,
        url: str,
        method: HttpMethod = HttpMethod.GET,
        params: dict = None,
        body: dict = None,
        headers: dict = None,
        auth = None):

        self.url = url
        self.method = method
        self.params = {} if not params else params
        self. body = {} if not body else body
        self.headers = {} if not headers else headers
        self.auth = auth


class HttpIngestor(Ingestor):
    """ HTTP ingestor """

    def __init__(self, url: str, stream_writer: StreamWriter = None):
        """ Constructor """

        super().__init__(stream_writer)

        self.http_request = HttpRequest(url=url)

        self.http_methods = {
            HttpMethod.GET: self._get,
            HttpMethod.POST: self._post
            }


    def with_method(self, method: HttpMethod) -> "HttpIngestor":
        """ HTTP method """
        self.http_request.method = method
        return self


    def with_params(self, params: dict) -> "HttpIngestor":
        """ Parameters """
        self.http_request.params = params
        return self


    def with_body(self, body: dict) -> "HttpIngestor":
        """ Body """
        self.http_request.body = body
        return self


    def with_headers(self, headers: dict) -> "HttpIngestor":
        """ Headers """
        self.http_request.headers = headers
        return self


    def with_auth(self, auth) -> "HttpIngestor":
        """ Authentication """
        self.http_request.auth = auth
        return self


    def _get(self, session, request, stream):
        return session.get(
            url=request.url,
            params=request.params,
            headers=request.headers,
            auth=request.auth,
            stream=stream)


    def _post(self, session, request, stream):
        return session.post(
            url=request.url,
            params=request.params,
            data=json.dumps(request.body),
            headers=request.headers,
            auth=request.auth,
            stream=stream)


    def ingest(self):
        """ Ingest """

        with requests.Session() as session:
            http_method = self.http_methods[self.http_request.method]
            response = http_method(session=session, request=self.http_request, stream=True)

            response.raise_for_status()

            if isinstance(response, ContextManager):
                with response as part:
                    part.raw.decode_content = True
                    self.stream_writer.write(part.raw)
            else:
                response.raw.decode_content = True
                self.stream_writer.write(response.raw)


class PaginationHandler:
    """ Paginated HTTP request pagination_handler """

    def __init__(
        self,
        page_size: int,
        page_size_param: str = "limit",
        data_property: str = "data",
        max_pages: int = 1000):

        self.page_size = page_size
        self.page_size_param = page_size_param
        self.data_property = data_property
        self.max_pages = max_pages


    def get_page_request(self, request: HttpRequest, page_number: int) -> HttpRequest:
        """ Compose HTTP request for a page """

        page_request = copy.deepcopy(request)
        page_request.params[self.page_size_param] = self.page_size
        page_request.params["offset"] = (page_number - 1) * self.page_size
        return page_request


    def is_last_page(self, payload: dict, page_number: int) -> bool:
        """ Check if last page """

        if page_number >= self.max_pages:
            return True

        if not self.data_property in payload.keys():
            return True

        if not isinstance(payload[self.data_property], list):
            return True

        if len(payload[self.data_property]) < self.page_size:
            return True

        return False


class PaginatedHttpIngestor(HttpIngestor):
    """ HTTP ingestor """

    def __init__(
        self,
        url: str,
        stream_writer: StreamWriter = None):

        super().__init__(url=url)
        self.pagination_handler = PaginationHandler(page_size=1000)
        self.stream_writer = stream_writer


    def with_page_size(self, page_size: int) -> "PaginatedHttpIngestor":
        """ Set page size """

        self.pagination_handler.page_size = page_size
        return self


    def with_page_size_param(self, page_size_param: str) -> "PaginatedHttpIngestor":
        """ Set page size parameter """

        self.pagination_handler.page_size_param = page_size_param
        return self


    def with_data_property(self, data_property: str) -> "PaginatedHttpIngestor":
        """ Set data property """

        self.pagination_handler.data_property = data_property
        return self


    def with_max_pages(self, max_pages: int) -> "PaginatedHttpIngestor":
        """ Set max pages """

        self.pagination_handler.max_pages = max_pages
        return self


    def ingest(self):
        """ Ingest """

        page_number = 1
        is_last_page = False

        while not is_last_page:

            page_request = self.pagination_handler \
                .get_page_request(self.http_request, page_number=page_number)

            with requests.Session() as session:
                http_method = self.http_methods[self.http_request.method]
                response = http_method(session=session, request=page_request, stream=False)

                response.raise_for_status()
                payload = response.json()

                hold_filename = self.stream_writer.get_filename()
                filename, ext = os.path.splitext(hold_filename)
                self.stream_writer.set_filename(f"{filename}.{page_number}{ext}")
                self.stream_writer.write_str(json.dumps(payload))
                self.stream_writer.set_filename(hold_filename)

            is_last_page = self.pagination_handler \
                .is_last_page(payload=payload, page_number=page_number)

            page_number += 1
