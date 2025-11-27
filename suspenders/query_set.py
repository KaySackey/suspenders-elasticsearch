import hashlib
import logging

from django.conf import settings

try:
    import rapidjson as json
except ImportError:
    import json

import typing
from elasticsearch import Elasticsearch
from functools import wraps
from funcy import cached_property

from .aggregations import TermsAggregation, TopHits
from .filters import (
    Filter,
    parse_filter,
    parse_kwargs,
)
from .queries import BoolQuery, ConstantScoreQuery, MatchAllQuery, MatchQuery, NegativeBoost, Query
from .result_set import ResultSet
from .utils import raw_count, raw_search, is_string_or_not_iterable
from .exceptions import ObjectDoesNotExist
logger = logging.getLogger("suspenders")


def clone(function):
    @wraps(function)
    def wrapped(self, *args, **kwargs):
        self = self.copy()
        return function(self, *args, **kwargs)

    return wrapped


class FilterDict:
    """
    Lists filters, and warns if we are overwriting an existing filter in debug mode

    Note using .update() will bypass warnings
    """

    def __init__(self, filters=None):
        self.filters = filters or {}

    def copy(self):
        next = FilterDict(filters=self.filters.copy())
        return next

    def __check(self, key, value, filter_type: str):
        if key in self.filters:
            prev_key_type, filter = self.filters[key]

            # If the filters can be merged (e.g. two terms filters), then do so and skip checking
            next_value = filter.merge(value)
            if next_value:
                return next_value

            message = (
                f"[{key}] already set within query set and is being overriden! "
                f"Previous {prev_key_type}{key} {filter.to_dict()} "
                f"Current: {filter_type}{key} {value.to_dict()}"
            )
            if settings.DEBUG:
                raise ValueError(message)
            else:
                logger.exception(message)

        return value

    def has_values(self) -> bool:
        return len(self.filters) > 0

    def clear_filters(self, key):
        if key in self.filters:
            del self.filters[key]

    def add_positive(self, key, value):
        if value.is_multi:
            key = f"+{key}"
        next_value = self.__check(key, value, "+")
        self.filters[key] = ("+", next_value)

    def add_negative(self, key, value):
        if value.is_multi:
            key = f"-{key}"
        next_value = self.__check(key, value, "-")
        self.filters[key] = ("-", next_value)

    def positive_filters(self) -> typing.List[Filter]:
        return [value for (filter_type, value) in self.filters.values() if filter_type == "+"]

    def negative_filters(self) -> typing.List[Filter]:
        return [value for (filter_type, value) in self.filters.values() if filter_type == "-"]


class SuspendersQuerySet:
    """Match documents based a selection of options
    A query set implicitly returns all documents in the database if no options are given
    """

    # Public variables
    minimum_should_match: int
    aggregations_use_sampler: bool

    # Private Variables
    _track_total_hits: bool
    _filters: FilterDict
    _sort: typing.List[typing.Dict[str, str]]
    _must_search_set: typing.List[Query]
    _optional_search_set: typing.List[Query]
    _negative_boost_queries: typing.List[Query]
    _aggs_fields: typing.List[TermsAggregation]
    _is_none_set: bool
    _result_set: typing.Optional[ResultSet]
    _min_score: typing.Optional[int]

    def __init__(self, conn):
        self.conn: Elasticsearch = conn

        self._min_score: int = None

        self._from: int = 0
        self._page_size: int = 10
        self._sort: typing.List[typing.Dict[str, str]] = []

        self._must_search_set: typing.List[Query] = []
        self._optional_search_set: typing.List[Query] = []

        # Dictionary that lists all the filters
        self._filters = FilterDict()

        self._negative_boost_queries: typing.List[Query] = []

        self._aggs_fields: typing.List[TermsAggregation] = []
        # If true, this query set will never be executed, and will always return an empty result set
        self._is_none_set: bool = False
        self._result_set: ResultSet = None
        self.minimum_should_match: int = 1
        self.aggregations_use_sampler = False
        self._track_total_hits: bool = False

    def copy(self):
        """Return a copy of this query set"""
        qs = self.__class__(self.conn)

        attrs = (
            "conn",
            "_min_score",
            "_from",
            "_page_size",
            "_sort",
            "_must_search_set",
            "_optional_search_set",
            "_filters",
            "_aggs_fields",
            "_negative_boost_queries",
            "minimum_should_match",
            "aggregations_use_sampler",
            "_track_total_hits",
            "_is_none_set",
        )

        for attr in attrs:
            value = getattr(self, attr)
            if isinstance(value, list):
                value = list(value)
            elif isinstance(value, FilterDict):
                value = value.copy()

            setattr(qs, attr, value)

        return qs

    def delete_by_query(self, indexes=None, **query_params):
        raise NotImplementedError

    def _execute_callback(self, result_set: ResultSet):
        """
        A callback function to be run on the ResultSet after it is returned from the database
        This is an internal function. Primarily used for the Django integration to wrap results in objects
        """
        return result_set

    def count(self, indexes=None) -> int:
        """
        Return the count of matched documents
        Evaluates the SuspendersQuerySet
        """

        # If we already executed then return cached values
        if self._result_set is not None:
            return self._result_set.total

        # If we cannot execute this query
        # Then return the empty set
        if self._is_none_set:
            return 0

        body = self.to_dict()

        count_body = {
            "query": body["query"],
        }

        for x in ["from"]:
            if x in body:
                count_body[x] = body[x]

        return raw_count(conn=self.conn, body=count_body, indexes=indexes)

    @property
    def documents(self):
        """
        Shortcut for just getting matched documents w/o any special execution on indexes
        """
        return self.execute().documents

    def execute(self, indexes=None, **query_params):
        """
        Execute the query and return a ResultSet
        """

        # If we already executed then return cached values
        if self._result_set is not None:
            return self._result_set

        # If we cannot execute this query
        # Then return the empty set
        if self._is_none_set:
            return self.empty_result_set()

        results = raw_search(
            conn=self.conn,
            body=self.to_dict(),
            indexes=indexes,
            callback=self._execute_callback,
            **query_params,
        )
        # Add to local cache
        self._result_set = results
        return self._result_set

    def first(self) -> typing.Optional[typing.Any]:
        """
        Return the first result of the query
        """

        result_set = self.execute()
        if len(result_set) > 0:
            return result_set.documents[0]
        return None

    def get(self, *args, **kwargs) -> typing.Any:
        """
        Return the first result of the query
        """
        qs = self.filter(*args, **kwargs)
        result = qs.first()
        if result is None:
            raise ObjectDoesNotExist()
        return result

    @staticmethod
    def empty_result_set():
        """
        Return an empty ResultSet
        """
        return ResultSet({"hits": {"hits": [], "total": 0, "max_score": 0}, "_shards": 0})

    def result_set_from_dict(self, a_dict):
        """
        Take a raw dictionary returned from ElasticSearch and turn it into a result set
        """
        results = ResultSet(a_dict)
        results = self._execute_callback(results)

        # Add to local cache
        self._result_set = results
        return self._result_set

    @cached_property
    def hash(self):
        return hashlib.sha1(str(self.to_dict()).encode("utf-8", "ignore")).hexdigest()

    def to_dict(self):
        if self._is_none_set:
            return {"is_none": True}

        # Read here: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html
        res = {
            "sort": self._sort or ["_score", "id"],
            "size": self._page_size,
            "query": self._serialized_query(),
            "track_total_hits": self._track_total_hits,
        }

        if self._min_score is not None:
            res["min_score"] = self._min_score

        if self._from:
            res["from"] = self._from

        if self._aggs_fields:
            # self._serialized_aggregations()
            if self.aggregations_use_sampler:
                res["aggregations"] = {
                    "__sampler__": {
                        "sampler": {"shard_size": 10},
                        "aggs": self._serialized_aggregations(),
                    }
                }
            else:
                res["aggregations"] = self._serialized_aggregations()

        return res

    def _serialized_query(self):
        """Return an dictionary in the form of ElasticSearch Query DSL"""
        has_search_context = self._must_search_set or self._optional_search_set
        has_filter_context = self._filters.has_values()

        if has_search_context:
            # Search context is expressed as a boolean
            res = BoolQuery(
                must=self._must_search_set,
                maybe=self._optional_search_set,
                # We do not implement must_not as part of the top-level query
                # Instead we force it into the filter context
                must_not=[],
                # minimum_should_match only makes sense with optional queries
                minimum_should_match=self.minimum_should_match
                if self._optional_search_set
                else None,
            )

            # If we have additional filters, we need to wrap them
            # into the filter context of a bool query
            # then execute the current query under that
            if has_filter_context:
                res = BoolQuery(
                    must=[res],
                    filter=BoolQuery(
                        must=self._filters.positive_filters(),
                        must_not=self._filters.negative_filters(),
                        acts_as_filter=True,
                    ),
                )
        else:
            # We can optimize due to the fact there's no outer query context
            if has_filter_context:
                res = ConstantScoreQuery(
                    filter=BoolQuery(
                        must=self._filters.positive_filters(),
                        must_not=self._filters.negative_filters(),
                        acts_as_filter=True,
                    )
                )
            else:
                # We are searching for everything
                res = MatchAllQuery()

        # Wrap query in a negative boost
        if self._negative_boost_queries:
            res = NegativeBoost(positive=res, negative=self._negative_boost_queries)

        return res.to_dict()

    @property
    def has_aggregations(self):
        return len(self._aggs_fields) > 0

    def _serialized_aggregations(self):
        """Return an dictionary in the form of ElasticSearch Filter DSL"""
        res = {}
        for agg in self._aggs_fields:
            res.update(agg.to_dict())

        return res

    @clone
    def min_score(self, score):
        self._min_score = score
        return self

    @clone
    def match_text(self, **kwargs):
        """A family of text queries that accept text, analyzes it, and constructs a query out of it."""
        key, values, options = parse_kwargs(kwargs)

        if is_string_or_not_iterable(values):
            values = [values]

        for value in values:
            # Values that end with a backslash aren't normal
            # value = value.rstrip("\\")

            q = MatchQuery(field=key, value=value, **options)
            self._must_search_set.append(q)

        return self

    @clone
    def add_optional_query(self, query: Query):
        """
        Add a should query directly to the set
        """
        self._optional_search_set.append(query)
        return self

    @clone
    def sorted_by(self, *orderings):
        """
        Sort the returned documents by these arguments
        Note:
            Adding _score is required if you want to score documents.
            For instance if you only use filters.
            Do not use scoring.
        """
        result = []

        for order in orderings:
            if order == "_score":
                # Sort score by descending order
                # as is ElasticSearch's default
                key = order
                value = "desc"
            elif order.startswith("_"):
                # Disallow using other special fields to sort
                raise ValueError(f"Invalid ordering {order}")
            elif order.startswith("-"):
                key = order[1:]
                value = "desc"
            elif order.startswith("+"):
                key = order[1:]
                value = "asc"
            else:
                key = order
                value = "asc"

            result.append({key: value})

        self._sort = result

        return self

    @clone
    def aggregation(
        self,
        term,
        *,
        size=10,
        min_doc_count=None,
        order=None,
        exclude=None,
        include=None,
        with_top_hits: TopHits = None,
        # Available options for term facet
        **kwargs,
    ):
        """Require faceting information for the result set.
        Facets are defined as kwargs k/v pairs in the format: facet_name=facet_obj

        Currently only term facets are implemented.

        Other facets depend too strongly on Pyes to be used directly.
        """
        if "name" in kwargs:
            del kwargs["name"]

        if isinstance(term, str):
            facet = TermsAggregation(
                term,
                name=term,
                size=size,
                order=order,
                exclude=exclude,
                include=include,
                min_doc_count=min_doc_count,
                with_top_hits=with_top_hits,
                **kwargs,
            )
        else:
            raise NotImplementedError(f"Aggregation does not understand type of term: {type(term)}")

        self._aggs_fields.append(facet)

        return self

    def __ensure_is_filter(self, value):
        if not isinstance(value, Filter):
            raise AttributeError("%s is not a filter class" % str(value))

    @clone
    def filter(self, *args, **kwargs):
        """Apply a positive filter on the result set

        Will match multiple kwargs
        Example:
            To only show items that are both made by someone they follow AND you have liked...

            qs = query_set.filter(created_by_id__in=following, favorite_by_user_id__in=following)

            This is identical to ...

            qs = query_set.filter(created_by_id__in=following).filter(favorite_by_user_id__in=following)

        """
        for f in self._make_filter_list(*args, **kwargs):
            self._filters.add_positive(key=f.name, value=f)
        return self

    @clone
    def exclude(self, *args, **kwargs):
        """Apply a negative filter (resolves to ES filter not) on the result set.
        Multiple calls to exclude will result in a boolean filter being used."""
        for f in self._make_filter_list(*args, **kwargs):
            self._filters.add_negative(key=f.name, value=f)

        return self

    @clone
    def suppress(self, *args, **kwargs):
        """
        Apply a negative boost to documents that match the produced query.

        """
        filters = self._make_filter_list(*args, **kwargs)
        for f in filters:
            self._negative_boost_queries.append(f)
        return self

    @clone
    def none(self):
        """Return a query set that matches no documents"""
        self._is_none_set = True
        return self

    @clone
    def no_pages(self):
        """
        Set size to zero
        """
        self._from = 0
        self._page_size = 0
        self._track_total_hits = False
        return self

    @clone
    def enable_hit_counter(self):
        """
        track hits
        """
        self._track_total_hits = True
        return self

    @clone
    def disable_hit_counter(self):
        """
        track hits
        """
        self._track_total_hits = False
        return self

    @clone
    def paginate(self, page=None, page_size=None):
        """Define the point to begin pagination from, and the # of rows each page returns
        Default page is 1
        Default page_size is 10.
        """

        page, page_size = int(page), int(page_size)

        if page <= 0:
            raise ValueError("Page must be > 0")
        if page_size <= 0:
            raise ValueError("Page_size must an integer > 0")

        if page is not None:
            self._from = (page - 1) * page_size
        if page_size is not None:
            self._page_size = page_size

        return self

    @clone
    def slice(self, start=None, end=None):
        """
        Take a slice start to end
        """

        start, end = int(start), int(end)
        page_size = end - start

        if 0 > start:
            raise ValueError("Start must be an integer greater than 0")
        if start > end:
            raise ValueError("End must be an integer greater than start")
        if 1 > page_size:
            raise ValueError("Derived page size must be at least 1")

        if start is not None:
            self._from = start
        if page_size is not None:
            self._page_size = page_size

        return self

    def _make_filter_list(self, *args, **kwargs) -> typing.List[Filter]:
        """
        Helper function. Take arguments and return a list of filters that could be created from it.
        """
        filters = []
        for key, value in kwargs.items():
            filters.append(parse_filter(**{key: value}))
        for value in args:
            self.__ensure_is_filter(value)
            filters.append(value)

        return filters

    #####################################
    # Magic Methods                     #
    #####################################

    def __repr__(self):
        """
        Return string representation of this SuspendersQuerySet
        Will not evaluate the query set.
        """
        return str(self.to_dict())

    def __str__(self):
        """ x.__str__() <==> str(x) """
        # Dump to a JSON string using double quotes
        return json.dumps(self.to_dict()).replace("'", '"')

    def __iter__(self):
        """ x.__iter__() <==> iter(x) """
        return self.execute().documents

    def __len__(self):
        """ x.__len__() <==> len(x) """
        return self.count()

    def __bool__(self):
        """Return true if this SuspendersQuerySet contains values"""
        return self.count() > 0

    @clone
    def all(self):
        """Return a copy of this query set
        Django compatabitliy
        """
        return self


class BoundSuspendersQuerySet(SuspendersQuerySet):
    default_indexes: typing.List[str]
    callback: typing.Callable[[ResultSet], ResultSet]

    def __init__(self, conn, indexes=None, callback=None):
        # Indexes is only none for compatability with super class
        super().__init__(conn)
        self.default_indexes = indexes
        self.callback = callback

    def copy(self):
        """Return a copy of this query set"""
        qs = super().copy()
        qs.default_indexes = self.default_indexes
        qs.callback = self.callback
        return qs

    def _execute_callback(self, result_set: ResultSet):
        """
        A callback function to be run on the ResultSet after it is returned from the database
        This is an internal function. Primarily used for the Django integration to wrap results in objects
        """
        return self.callback(result_set)

    def delete_by_query(self, indexes=None, **query_params):
        indexes = self.default_indexes if not indexes else indexes
        return super().delete(indexes=indexes, **query_params)

    def execute(self, indexes=None, **query_params):
        indexes = self.default_indexes if not indexes else indexes
        return super().execute(indexes=indexes, **query_params)

    def count(self, indexes=None):
        indexes = self.default_indexes if not indexes else indexes
        return super().count(indexes)
