import io
import json
from unittest import mock


def mock_response(
        status=200,
        content="CONTENT",
        json_data=None,
        raise_for_status=None):
    """ Compose mock response """

    mock_resp = mock.Mock()

    mock_resp.raise_for_status = mock.Mock()
    if raise_for_status:
        mock_resp.raise_for_status.side_effect = raise_for_status

    mock_resp.status_code = status
    mock_resp.content = content

    if json_data:
        mock_resp.json.return_value = json_data
        mock_resp.content = json.dumps(json_data)

    mock_resp.raw = io.BytesIO(mock_resp.content.encode("utf-8"))

    return mock_resp
