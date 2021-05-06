import datetime
import time
import os
import requests

DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"

# session = requests.Session()
# logger = singer.get_logger()

class KlaviyoError(Exception):
    pass

class KlaviyoNotFoundError(KlaviyoError):
    pass

class KlaviyoBadRequestError(KlaviyoError):
    pass

class KlaviyoUnauthorizedError(KlaviyoError):
    pass

class KlaviyoForbiddenError(KlaviyoError):
    pass

class KlaviyoInternalServiceError(KlaviyoError):
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": KlaviyoBadRequestError,
        "message": "Request is missing or has a bad parameter."
    },
    401: {
        "raise_exception": KlaviyoUnauthorizedError,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": KlaviyoForbiddenError,
        "message": "Invalid authorization credentials or permissions."
    },
    404: {
        "raise_exception": KlaviyoNotFoundError,
        "message": "The requested resource doesn't exist."
    },
    500: {
        "raise_exception": KlaviyoInternalServiceError,
        "message": "Internal Service Error from Klaviyo."
    }
}

def raise_for_error(response):   
    try:
        response.raise_for_status()
    except requests.HTTPError:
        try:
            json_resp = response.json()
        except (ValueError, TypeError, IndexError, KeyError):
            json_resp = {}

        error_code = response.status_code
        message_text = json_resp.get("message", ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("message", "Unknown Error"))
        message = "HTTP-error-code: {}, Error: {}".format(error_code, message_text)
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", KlaviyoError)
        raise exc(message) from None

def dt_to_ts(dt):
    return int(time.mktime(datetime.datetime.strptime(
        dt, DATETIME_FMT).timetuple()))


def ts_to_dt(ts):
    return datetime.datetime.fromtimestamp(
        int(ts)).strftime(DATETIME_FMT)


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)
