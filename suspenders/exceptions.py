from pprint import pformat

from elasticsearch import NotFoundError

__all__ = [
    "NotFoundError",
    "NoServerAvailable",
    "SuspendersException",
    "IndexMissingException",
    "TooComplexToDetermine",
    "UnknownError",
    "InvalidQuerySet"
]


class SuspendersException(Exception):
    pass

class ObjectDoesNotExist(SuspendersException):
    """
    The requested document does not exist.
    """
    pass

class UnformattedError(SuspendersException):
    error: str

    def __init__(self, error, status=None, request_body=None):
        self.error = error
        self.status = status
        self.request_body = request_body

    def __str__(self):
        # In this case error is a string
        reason = self.error
        body = pformat(self.request_body, width=80) if self.request_body else ""
        return f"<SuspendersException: Unknown> {reason}\n {body}"


class UnknownError(UnformattedError):
    """
    Represents an unformatted error returend from the ElasticSearch client
    """

    pass


class NoServerAvailable(UnformattedError):
    pass


class SearchError(SuspendersException):
    """
    Base class of exceptions raised as a result of parsing an error return
    from ElasticSearch.

    An exception with this class will be raised if no more specific subclass is
    appropriate.

      {
        "type" : "resource_already_exists_exception",
        "reason" : "index [twitter/E7AzBiS3QGqrIqO5xWnKRw] already exists",
        "index_uuid" : "E7AzBiS3QGqrIqO5xWnKRw",
        "index" : "twitter"
      }

    """

    error: dict

    def __init__(self, error, status=None, request_body=None):
        self.error = error
        self.status = status
        self.request_body = request_body

    def __str__(self):
        err_type = self.error["type"]
        reason = self.error["reason"]
        body = pformat(self.request_body, width=80) if self.request_body else ""
        return f"<SearchError: {err_type}> {reason}\n {body}"


class TooComplexToDetermine(SearchError):
    pass


class IndexMissingException(SearchError):
    pass


class DocumentMissingException(SearchError, ObjectDoesNotExist):
    pass


class InvalidQuerySet(TypeError):
    pass
