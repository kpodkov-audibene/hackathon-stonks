import requests
import logging
from tenacity import Retrying, RetryError
import tenacity


class Client:

    def __init__(self, stop_after_delay=300, stop_after_attempt=5, continue_on_error=False):
        self.log = logging.getLogger('http')
        self.log.setLevel(logging.INFO)
        self.client = requests
        # Retry params
        self.stop_after_delay = stop_after_delay
        self.stop_after_attempt = stop_after_attempt
        self.continue_on_error = continue_on_error

    def retry(f):
        def wrapped_f(self, *args, **kwargs):
            try:
                for attempt in Retrying(
                        stop=(tenacity.stop_after_delay(self.stop_after_delay) | tenacity.stop_after_attempt(
                            self.stop_after_attempt)),
                        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10)):
                    with attempt:
                        return f(self, *args, **kwargs)
            except RetryError as e:
                if not self.continue_on_error:
                    e.reraise()
                else:
                    for error in e.args:
                        self.log.warning("[http.{}] {}".format(f.__name__, error))
                    self.log.warning(
                        "[http.{}] Continuing after exhausting [{}] retries...".format(f.__name__,
                                                                                       self.stop_after_attempt))

        return wrapped_f

    @retry
    def get(self, url, params='', headers='', stream=False, timeout=(15.05, 360)):
        result = self.client.get(url, params=params, headers=headers, stream=stream, timeout=timeout)
        if result.status_code not in range(200, 299):
            self.log.error("Bad status code [{0}] for url {1}".format(result.status_code, result.url))
            self.log.error(result.content)
            raise ConnectionError("Bad status code [{0}] for url {1}".format(result.status_code, result.url))
        else:
            return result

    @retry
    def post(self, url, headers='', payload='', params='', files='', timeout=(15.05, 360)):
        result = self.client.post(url, params=params, files=files, data=payload, headers=headers, timeout=timeout)
        if result.status_code not in range(200, 299):
            self.log.error("Bad status code [{0}] for url {1}".format(result.status_code, result.url))
            self.log.error(result.content)
            raise ConnectionError("Bad status code [{0}] for url {1}".format(result.status_code, result.url))
        else:
            return result

    @retry
    def delete(self, url, headers='', payload='', params='', timeout=(15.05, 360)):
        result = self.client.delete(url, params=params, data=payload, headers=headers, timeout=timeout)
        if result.status_code not in range(200, 299):
            self.log.error("Bad status code [{0}] for url {1}".format(result.status_code, result.url))
            self.log.error(result.content)
            raise ConnectionError(
                "Bad status code [{0}] for url {1}".format(result.status_code, result.url))
        else:
            return result
