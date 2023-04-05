""" HTTP Request callable """

from io import BytesIO
import json
from typing import Callable, ContextManager
from enum import Enum
import copy
import requests
from xcputils.batch import Batch
from xcputils.ingestion import Ingestor

from xcputils.streaming import StreamConnector

def get(url: str,
        connector: StreamConnector,
        params: dict = None,
        headers: dict = None,
        auth=None) -> requests.Response:
    """ HTTP GET request """

    with requests.Session() as session:
        response = session.get(url=url,
                               params=params,
                               headers=headers,
                               auth=auth,
                               stream=True)
        response.raise_for_status()
        with response as part:
            part.raw.decode_content = True
            connector.write(part.raw)
    return response


def post(url: str,
        body: dict,
        connector: StreamConnector,
        params: dict = None,
        headers: dict = None,
        auth=None) -> requests.Response:
    """ HTTP POST request """

    with requests.Session() as session:
        response = session.post(url=url,
                                data=json.dumps(body),
                                params=params,
                                headers=headers,
                                auth=auth,
                                stream=True)
        response.raise_for_status()
        with response as part:
            part.raw.decode_content = True
            connector.write(part.raw)
    return response


def get_paginated(url: str,
    create_connector: Callable[[int], StreamConnector],
    limit: int,
    maximum = int,
    limit_param: str = "limit",
    offset_param: str = "offset",
    params: dict = None,
    headers: dict = None,
    auth = None) -> None:
    """ Paginated HHTP GET """

    batch = Batch()
    for offset in list(range(0, maximum, limit)):
        page_params = {key: value for key, value in params.items()}
        page_params[limit_param] = limit
        page_params[offset_param] = offset
        batch.append(get,
            url,
            create_connector(offset),
            page_params,
            headers,
            auth)
    responses = batch()
    for r in responses:
        if isinstance(r, requests.HTTPError):
            raise r
        if isinstance(r, Exception):
            raise r


def post_paginated(url: str,
    body: dict,
    create_connector: Callable[[int], StreamConnector],
    limit: int,
    maximum = int,
    limit_param: str = "limit",
    offset_param: str = "offset",
    params: dict = None,
    headers: dict = None,
    auth = None) -> None:
    """ Paginated HHTP POST """

    batch = Batch()
    for offset in list(range(0, maximum, limit)):
        page_params = {key: value for key, value in params.items()}
        page_params[limit_param] = limit
        page_params[offset_param] = offset
        batch.append(post,
            url,
            body,
            create_connector(offset),
            page_params,
            headers,
            auth)
    responses = batch()
    for response in responses:
        if isinstance(response, requests.HTTPError):
            raise response
        if isinstance(response, Exception):
            raise response


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

    def __init__(self, request: HttpRequest, stream_connector: StreamConnector):
        """ Constructor """

        super().__init__(stream_connector)
        self.request = request
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
            http_method = self.http_methods[self.request.method]
            response = http_method(session=session, request=self.request, stream=True)

            response.raise_for_status()

            if isinstance(response, ContextManager):
                with response as part:
                    part.raw.decode_content = True
                    self.stream_connector.write(part.raw)
            else:
                response.raw.decode_content = True
                self.stream_connector.write(response.raw)


class PaginationHandler:
    """ Paginated HTTP request handler """

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
        page_request.params["offset"] = page_number * self.page_size
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
        request: HttpRequest,
        stream_connector: StreamConnector,
        handler: PaginationHandler,
        file_pattern: str = "page-{page_number}.json"):
        """ Constructor """

        super().__init__(request=request, stream_connector=stream_connector)

        self.handler = handler
        self.file_pattern = file_pattern


    def ingest(self):
        """ Ingest """

        page_number = 1
        is_last_page = False

        while not is_last_page:

            page_request = self.handler.get_page_request(self.request, page_number=page_number)
            self.stream_connector.file_name = self.file_pattern.replace("{page_number}", str(page_number))

            with requests.Session() as session:
                http_method = self.http_methods[self.request.method]
                response = http_method(session=session, request=page_request, stream=False)

                response.raise_for_status()
                payload = response.json()

            with BytesIO() as stream:
                stream.write(json.dumps(payload).encode('utf-8'))
                stream.seek(0)
                self.stream_connector.write(stream)

            is_last_page = self.handler.is_last_page(payload=payload, page_number=page_number)
            page_number += 1
