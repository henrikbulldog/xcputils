""" Package xcputils """

from xcputils.ingestion.http import HttpIngestor, HttpMethod, HttpRequest


class XCPUtils():
    """ Utilities for copying data """


    def read_from_http(self,
        url: str,
        method: HttpMethod = HttpMethod.GET,
        params: dict = None,
        body: dict = None,
        headers: dict = None,
        auth = None,
        ) -> HttpIngestor:
        """ Read from HTTP """

        return HttpIngestor(
            http_request=HttpRequest(
                url=url,
                method=method,
                params=params,
                body=body,
                headers=headers,
                auth=auth)
        )