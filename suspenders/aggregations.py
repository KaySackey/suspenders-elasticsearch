import typing


class TopHits:
    """
    The top_hits aggregator can effectively be used to group result sets by certain fields via a bucket aggregator.

    from - The offset from the first result you want to fetch.
    size - The maximum number of top matching hits to return per bucket. By default the top three matching hits are returned.
    sort - How the top matching hits should be sorted. By default the hits are sorted by the score of the main query.
    """

    _sort: typing.Dict[str, str]

    def __init__(
        self,
        *,
        # List of sorts e.g. ({'modified': 'desc'}, {'id': 'desc'})
        # This is found in Queryset._sort if you need it
        sort,
        size=10,
        name="top_hits",
    ):
        self.name = name
        self.size = size
        self._sort = sort

    def serialize(self):
        """
        Used as a sub-aggregation; attached to a real aggregation

        Example Return: https://www.elastic.co/guide/en/elasticsearch/reference/1.7/search-aggregations-metrics-top-hits-aggregation.html

        "aggregations": {
            "top_tag_hits": {
                "top_hits": {
                    "sort": [
                        {
                            "last_activity_date": {
                                "order": "desc"
                            }
                        }
                    ],
                    "_source": {
                        "include": [
                            "title"
                        ]
                    },
                    "size" : 1
                }
            }
        }
        """
        data = {"top_hits": {"sort": self._sort, "size": self.size}}

        return {self.name: data}


class TermsFilter:
    """
    This is actually an aggregation
    Updated as of 2.27.2018 - Facets are deprecated and we're running on ES 1.7 which have aggregations
    """

    def __init__(
        self,
        field,
        name=None,
        size=10,
        order=None,
        include=None,
        exclude=None,
        min_doc_count=None,
        regex=None,
        regex_flags="DOTALL",
        script=None,
        with_top_hits: TopHits = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.name = name
        self.field = field
        if name is None:
            self.name = field
        self.size = size
        self.order = order
        self.include = include
        self.exclude = exclude or []
        self.regex = regex
        self.regex_flags = regex_flags
        self.script = script
        self.min_doc_count = min_doc_count
        self.with_top_hits = with_top_hits

    def to_dict(self):
        """Compatability with elasticsearch-dsl"""
        return self.serialize()

    def serialize(self):
        data = {"field": self.field}

        if self.min_doc_count is not None:
            data["min_doc_count"] = self.min_doc_count

        if self.size:
            data["size"] = self.size

        if self.order:
            if self.order not in ["count", "term", "reverse_count", "reverse_term"]:
                raise RuntimeError("Invalid order value:%s" % self.order)
            data["order"] = self.order
        if self.include:
            data["include"] = self.include
        if self.exclude:
            data["exclude"] = self.exclude
        if self.regex:
            data["regex"] = self.regex
            if self.regex_flags:
                data["regex_flags"] = self.regex_flags
        elif self.script:
            data["script"] = self.script

        inner = {}
        inner["terms"] = data

        if self.with_top_hits:
            inner["aggregations"] = self.with_top_hits.serialize()

        return {self.name: inner}
