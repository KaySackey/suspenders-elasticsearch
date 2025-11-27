import collections
import logging
from contextlib import contextmanager

import urllib3
from elasticsearch import Elasticsearch, TransportError

from .exceptions import (
    DocumentMissingException,
    IndexMissingException,
    NoServerAvailable,
    SearchError,
    TooComplexToDetermine,
    UnknownError,
)
from .result_set import ResultSet

logger = logging.getLogger("suspenders")


def raw_search(*, conn: Elasticsearch, body, indexes, callback=None, **query_params) -> ResultSet:
    with handle_elastic_search_errors(body):
        raw_results = conn.search(body=body, index=indexes, **query_params)
        # parse into a result set
        results = ResultSet(raw_results)
        # refine if needed
        return callback(results) if callback else results


def raw_count(*, conn: Elasticsearch, body, indexes, callback=None, **query_params) -> int:
    with handle_elastic_search_errors(body=f"COUNT {indexes}"):
        raw_results = conn.count(body=body, index=indexes, **query_params)
        return raw_results["count"]


@contextmanager
def handle_elastic_search_errors(body):
    """
    For all errors it will log the problem, then retry the task given by celery_task
    Returns the result of func() or False on an unhandled error
    """
    try:
        yield
    except (IOError, urllib3.exceptions.HTTPError) as ex:
        raise NoServerAvailable(error=str(ex), status=500, request_body=body)
    except TransportError as e:
        raise_on_transport_error(e, body)
    except Exception:
        raise


def raise_on_transport_error(e, request_body: str):
    # HTTP_EXCEPTIONS = {
    #     400: RequestError,
    #     401: AuthenticationException,
    #     403: AuthorizationException,
    #     404: NotFoundError,
    #     409: ConflictError,
    # }
    result = e.info
    status = e.status_code

    if not isinstance(result, dict) or "error" not in result:
        logger.exception(f"Unknown error from ElasticSearch: {str(e)} {str(result)}")
        raise UnknownError(error=str(result), status=status, request_body=request_body)

    raw_error = result["error"]

    try:
        error_list = raw_error["root_cause"]

        for error in error_list:
            __match_error(error, status, request_body)
    except TypeError:
        raise ValueError(raw_error)


def __match_error(error: dict, status: int, request_body: str):
    """
    Example dict
        "type" : "resource_already_exists_exception",
        "reason" : "index [twitter/E7AzBiS3QGqrIqO5xWnKRw] already exists",
        "index_uuid" : "E7AzBiS3QGqrIqO5xWnKRw",
        "index" : "twitter"

    :param error:
    :return:
    """
    error_type = error["type"]

    if error_type == "index_not_found_exception":
        ErrorClass = IndexMissingException
    elif error_type == "document_missing_exception":
        ErrorClass = DocumentMissingException
    elif error_type == "too_complex_to_determinize_exception":
        ErrorClass = TooComplexToDetermine
    else:
        ErrorClass = SearchError

    raise ErrorClass(error=error, status=status, request_body=request_body)


def is_string_or_not_iterable(arg):
    return isinstance(arg, str) or not isinstance(arg, collections.Iterable)
