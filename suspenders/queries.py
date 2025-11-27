import typing

from suspenders.exceptions import InvalidQuerySet


class Query:
    def to_dict(self):
        """Return a dictionary representing this query"""
        return {}


class NegativeBoost(Query):
    def __init__(self, negative, positive=None, negative_boost=0.5):
        """
        Unlike the standard query, we only take negative fields here.
        """
        self.positive = positive
        self.negative = negative
        self.negative_boost = negative_boost

    def _serialize_inner_queries(self, inner_query_set):
        if isinstance(inner_query_set, dict):
            return inner_query_set
        return [query.to_dict() for query in inner_query_set]

    def to_dict(self):
        """
        Online Documentation: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-boosting-query.html

        Example Outputs:
            {
                "boosting" : {
                    "positive" : {
                        "term" : {
                            "field1" : "value1"
                        }
                    },
                    "negative" : {
                        "term" : {
                            "field2" : "value2"
                        }
                    },
                    "negative_boost" : 0.2
                }
            }
        """
        inner = {"negative_boost": self.negative_boost}

        if self.positive:
            inner["positive"] = self._serialize_inner_queries(self.positive)
        if self.negative:
            inner["negative"] = self._serialize_inner_queries(self.negative)

        res = {"boosting": inner}

        return res


class TermQuery(Query):
    def __init__(self, field, value):
        self.field = field
        self.value = value

        # Todo: Handle boost and other options

    def to_dict(self):
        """
        Online Documentation: http://www.elasticsearch.com/docs/elasticsearch/rest_api/query_dsl/term_query/

        Example Outputs:
             {
                "term" : { "user" : { "value" : "kimchy", "boost" : 2.0 } }
             }
        """

        res = {"term": {self.field: self.value}}

        return res


class MatchAllQuery(Query):
    def to_dict(self):
        """
        Online Documentation: http://www.elasticsearch.com/docs/elasticsearch/rest_api/query_dsl/match_all_query/

        Example Outputs:
        [1]
            {
                "match_all" : { }
            }
        [2]
            {
                "match_all" : { "boost" : 1.2 }
            }
        """

        return {"match_all": {}}


class ConstantScoreQuery(Query):
    def __init__(self, filter):
        if not filter:
            raise ValueError("Cannot run constant score without a filter")
        self.filter = filter

    def to_dict(self):
        """
        Online Documentation: https://www.elastic.co/guide/en/elasticsearch//reference/current/query-dsl-constant-score-query.html

        Example Outputs:
        {
            "query": {
                "constant_score" : {
                    "filter" : {
                        "term" : { "user" : "kimchy"}
                    },
                    "boost" : 1.2
                }
            }
        }
        """

        return {
            "constant_score": {
                "filter": self.filter.to_dict(),
            }
        }


class FuzzyQuery(Query):
    def __init__(self, field, value, boost=1.0, prefix_length=0):
        self.field = field
        self.value = value
        self.boost = boost
        self.prefix_length = prefix_length

    def to_dict(self):
        """
        Online Documentation: https://www.elastic.co/guide/en/elasticsearch/reference/7.3/query-dsl-fuzzy-query.html#_string_fields_2

        Example Outputs:
        [1]
            {
                "fuzzy" : { "user" : "ki" }
            }
        [2]
            {
                "fuzzy" : {
                    "user" : {
                        "value" : "ki",
                        "boost" : 1.0,
                        "prefix_length" : 0
                    }
                }
            }
        """

        res = {
            "fuzzy": {
                self.field: {
                    "value": self.value,
                    "boost": self.boost,
                    "prefix_length": self.prefix_length,
                }
            }
        }

        return res


class MultiMatchQuery(Query):
    def __init__(
        self,
        *,
        fields: typing.List[str],
        value: str,
        search_type="phrase_prefix",  # acts like phrase_prefix on best_fields
        tie_breaker=1.0,
        operator="or",
        **extra
    ):
        self.fields = fields
        self.value = value
        self.search_type = search_type
        self.tie_breaker = tie_breaker
        self.operator = operator
        self.extra = extra

    def to_dict(self):
        """
        Online Documentation:

        https://www.elastic.co/guide/en/elasticsearch/reference/1.7/query-dsl-multi-match-query.html
        Example Outputs:

        [1]
            {
              "multi_match" : {
                "query":      "brown fox",
                "type":       "best_fields",
                "fields":     [ "subject", "message" ],
                "tie_breaker": 0.3
              }
            }
        """

        res = {
            "multi_match": {
                "query": str(self.value),
                "fields": self.fields,
                "type": self.search_type,
                "tie_breaker": self.tie_breaker,
                "operator": self.operator,
                **self.extra,
            }
        }

        return res


class MatchQuery(Query):
    def __init__(self, field, value, search_type="boolean", max_expansions=50):
        self.field = field
        self.value = value
        self.search_type = search_type
        self.max_expansions = max_expansions

    def to_dict(self):
        res = {
            "match": {
                self.field: {
                    "query": self.value,
                    "max_expansions": self.max_expansions,
                }
            }
        }

        return res


class MatchPhrasePrefixQuery(Query):
    def __init__(self, field, value, search_type="boolean", max_expansions=50):
        self.field = field
        self.value = value
        self.max_expansions = max_expansions

    def to_dict(self):
        # Documentation: https://www.elastic.co/guide/en/elasticsearch/reference/7.3/query-dsl-match-query-phrase-prefix.html
        res = {
            "match_phrase_prefix": {
                self.field: {
                    "query": self.value,
                    "max_expansions": self.max_expansions,
                }
            }
        }

        return res


class BoolQuery(Query):
    def __init__(
        self,
        options=None,
        must=None,
        maybe=None,
        must_not=None,
        filter=None,
        minimum_should_match=None,
        # If this query is acting as a filter
        # then scoring is ignored anyways
        # so we don't need to worry about things
        acts_as_filter=False,
    ):
        self.name = hash(self)
        self.must = must
        self.maybe = maybe
        self.must_not = must_not
        self.filter = filter
        self.minimum_should_match = minimum_should_match

        if minimum_should_match and not maybe:
            raise ValueError(
                "minimum_should_match can only be used in conjuction with optional matches."
            )

        if must_not and not (must or maybe or acts_as_filter):
            raise InvalidQuerySet("It is not possible to search on documents that only consists of a must_not clauses")

        if not (must or maybe or must_not):
            raise InvalidQuerySet("A must or should clause is necessary to execute a Boolean Query")

        self.options = options if options else {}

    def _serialize_inner_queries(self, inner_query_set):
        return [query.to_dict() for query in inner_query_set]

    def to_dict(self):
        """
        Example Outputs:
        [1]
            {
                "bool" : {
                    "must" : {
                        "term" : { "user" : "kimchy" }
                    },
                    "must_not" : {
                        "range" : {
                            "age" : { "from" : 10, "to" : 20 }
                        }
                    },
                    "should" : [
                        {
                            "term" : { "tag" : "wow" }
                        },
                        {
                            "term" : { "tag" : "elasticsearch" }
                        }
                    ],
                    "minimum_should_match" : 1,
                    "boost" : 1.0
                }
            }
        """

        inner = {}

        if self.minimum_should_match:
            inner["minimum_should_match"] = self.minimum_should_match
        if self.must:
            inner["must"] = self._serialize_inner_queries(self.must)
        if self.maybe:
            inner["should"] = self._serialize_inner_queries(self.maybe)
        if self.must_not:
            inner["must_not"] = self._serialize_inner_queries(self.must_not)
        if self.filter:
            inner["filter"] = self.filter.to_dict()

        res = {"bool": inner}

        return res
