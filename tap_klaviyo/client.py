import backoff
import requests
import simplejson
from singer import metrics
from utils import raise_for_error, KlaviyoInternalServiceError

session = requests.Session()

@backoff.on_exception(backoff.expo, (simplejson.scanner.JSONDecodeError, KlaviyoInternalServiceError), max_tries=3)
def authed_get(source, url, params):
    with metrics.http_request_timer(source) as timer:
        resp = session.request(method='get', url=url, params=params)

        if resp.status_code != 200:
            raise_for_error(resp)
        else:
            resp.json()
            timer.tags[metrics.Tag.http_status_code] = resp.status_code
            return resp