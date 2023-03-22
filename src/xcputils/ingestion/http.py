""" HTTP Request callable """

import json
from typing import Callable
import requests
from xcputils.batch import Batch

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
    for r in responses:
        if isinstance(r, requests.HTTPError):
            raise r
        if isinstance(r, Exception):
            raise r
