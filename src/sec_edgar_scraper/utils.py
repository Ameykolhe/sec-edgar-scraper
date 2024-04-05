import http
import time

import requests

from sec_edgar_scraper.exceptions import GetRequestException, TooManyRequestException


def make_get_request(url, headers, retry_count=0, payload=None, resp_json=True) -> dict | str:
    """
        Performs a GET request to a specified URL with the option to automatically
        retry on encountering a 429 Too Many Requests HTTP status code. It can return
        the response either as a JSON object or a string, based on the `resp_json` flag.

        Parameters:
            url (str): The URL to which the GET request is sent.
            headers (dict): Headers to include in the GET request.
            retry_count (int, optional): The current count of retry attempts for the
                                         request. Automatically managed during recursion.
                                         Defaults to 0.
            payload (dict, optional): An optional payload to send with the request.
                                      Defaults to None.
            resp_json (bool, optional): Determines whether the response should be returned
                                        as a JSON object (True) or a raw string (False).
                                        Defaults to True.

        Returns:
            dict | str: If `resp_json` is True, returns the response JSON object. If False,
                        returns the response content as a decoded string.

        Raises:
            TooManyRequestException: If the server returns a 429 Too Many Requests status,
                                     indicating rate limiting. The function will retry the
                                     request, incrementing the retry count.
            GetRequestException: If any other request exception occurs, or if the retry
                                 count is exhausted without a successful response.

        Note:
            This function includes a simple exponential backoff mechanism by sleeping
            for 0.5 seconds initially, then for a period equal to the retry count + 1
            seconds on subsequent retries if a 429 status code is encountered. This
            helps to manage rate limits imposed by the server.
    """
    try:

        if retry_count < 3:
            time.sleep(0.5)
            resp = requests.get(url, headers=headers, data=payload)
            if resp.status_code == http.HTTPStatus.TOO_MANY_REQUESTS:
                raise TooManyRequestException("Http Status code = 429")
            if resp_json:
                return resp.json()
            else:
                return resp.content.decode("utf-8")
        else:
            raise GetRequestException(f"Retry count exhausted - {url}")

    except TooManyRequestException:
        time.sleep(retry_count + 1)
        make_get_request(url, headers, retry_count + 1)

    except GetRequestException as e:
        raise e

    except Exception as e:
        raise GetRequestException(str(e))
