""" HTTP Request callable """

import json
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

    def __init__(self, http_request: HttpRequest, stream_writer: StreamWriter):
        """ Constructor """
        super().__init__(stream_writer)
        self.http_request = http_request
        self.http_methods = {
            HttpMethod.GET: self._get,
            HttpMethod.POST: self._post
            }


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
        http_request: HttpRequest,
        pagination_handler: PaginationHandler,
        get_stream_writer: Callable[[int], StreamWriter]):
        """ Constructor """

        super().__init__(http_request=http_request, stream_writer=None)

        self.pagination_handler = pagination_handler
        self.get_stream_writer = get_stream_writer


    def ingest(self):
        """ Ingest """

        page_number = 1
        is_last_page = False

        while not is_last_page:

            page_request = self.pagination_handler.get_page_request(self.http_request, page_number=page_number)

            with requests.Session() as session:
                http_method = self.http_methods[self.http_request.method]
                response = http_method(session=session, request=page_request, stream=False)

                response.raise_for_status()
                payload = response.json()

                stream_writer = self.get_stream_writer(page_number)
                stream_writer.write_str(json.dumps(payload))

            is_last_page = self.pagination_handler.is_last_page(payload=payload, page_number=page_number)
            page_number += 1
