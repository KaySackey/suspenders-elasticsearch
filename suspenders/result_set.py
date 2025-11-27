import typing

if typing.TYPE_CHECKING:
    from suspenders.mappings import BaseMap


class Result:
    def __init__(self, doc):
        doc = doc.copy()
        self._attributes = []

        # Set attributes directly on this as a convenience
        for key, value in doc.items():
            setattr(self, key, value)
            self._attributes.append(key)

    def as_dict(self):
        doc = {}
        for key in self._attributes:
            # Keys are strings so the dictionary can easily be used as a **kwarg
            doc[str(key)] = getattr(self, key)

        return doc

    def __str__(self):
        return str(self.as_dict())

    def __repr__(self):
        return str(self)


class ResultSet:
    """Contains the results of the matched set of documents from ElasticSearch"""

    # If true this result set came from the cache
    from_cache = None

    def __init__(self, response):
        """
            Response is expected to be a dictionary containing results from ElasticSearch

            Sample Response:
                http://www.elasticsearch.com/docs/elasticsearch/rest_api/search/body_request/
            [1]
            {
                "_shards":{
                    "total" : 5,
                    "successful" : 5,
                    "failed" : 0
                },
                "hits":{
                    "total" : 1,
                    "hits" : [
                        {
                            "_index" : "twitter",
                            "_type" : "tweet",
                            "_id" : "1",
                            "_source" : {
                                "user" : "kimchy",
                                "postDate" : "2009-11-15T14:12:12",
                                "message" : "trying out ElasticSearch"
                            }
                        }
                    ]
                }
            }

        {u'_shards': {u'failed': 0, u'successful': 5, u'total': 5},
         u'hits': {u'hits': [], u'max_score': None, u'total': 0},
         u'took': 2}

        """

        self._raw = response
        self._hits = response["hits"]
        self._documents = response["hits"]["hits"]
        self._shards = response["_shards"]

        if "total" in self._hits:
            if isinstance(self._hits["total"], int):
                # Using ES6 and below
                self.total = self._hits["total"]
                self.total_relation = "eq"
            else:
                # Using ES7 and above
                # Tracking total hits
                self.total = self._hits["total"]["value"]
                # total relation can be
                # eq -> we are 100% accurate
                # gte -> this is the lower bound; we have more docs
                self.total_relation = self._hits["total"]["relation"]
        else:
            # _track_total_hits was off
            self.total = None

        self.max_score = self._hits["max_score"]
        self.aggregations = {}
        if "aggregations" in response:
            # aggregations are accessed as a dictionary where:
            # facet[name] returns a list of the facet terms and their buckets

            # Only handles term facet now
            ## Example response:
            #
            # "aggregations": {
            #     "genders": {
            #         "doc_count_error_upper_bound": 0,
            #         "sum_other_doc_count"        : 0,
            #         "buckets"                    : [
            #             {
            #                 "key"      : "male",
            #                 "doc_count": 10
            #             },
            #             {
            #                 "key"      : "female",
            #                 "doc_count": 10
            #             },
            #         ]
            #     }
            # }
            ##

            # Check if we have a sampler
            # And enter it
            if "__sampler__" in response["aggregations"]:
                aggregation_items = response["aggregations"]["__sampler__"]
            else:
                aggregation_items = response["aggregations"]

            for name, results in aggregation_items.items():
                if not isinstance(results, dict):
                    continue
                self.aggregations[name] = results["buckets"]  # results["buckets"]

        self.explain = []
        self._objects = []
        self._object_dictionary = []

    @property
    def documents(self):
        return self.as_objects()

    def __iter__(self):
        for x in self.as_objects():
            yield x

    def __getitem__(self, k: typing.Union[int, slice]):
        """
        Retrieve an item or slice from the set of results.
        Does not allow negative indexing.
        """
        if not isinstance(k, (int, slice)):
            raise TypeError

        # Do not allow negative indexing
        assert (not isinstance(k, slice) and (k >= 0)) or (
            isinstance(k, slice)
            and (k.start is None or k.start >= 0)
            and (k.stop is None or k.stop >= 0)
        ), "Negative indexing is not supported."

        return self.documents[k]

    def __len__(self):
        return len(self._documents)

    def __bool__(self):
        return len(self._documents) == 0

    def _clean_doc_value(self, value):
        if type(value) is dict:
            ret = {}
            for k in value:
                # Keys are strings because we may eventually just use the dictionary as kwargs in DJango
                ret[str(k)] = self._clean_doc_value(value[k])

            return ret

        return value

    def _clean_doc(self, doc):
        doc = doc.copy()
        cleaned_doc = {}

        for field_name in ["_source", "fields"]:
            # Aggregate fields from different sources
            if field_name in doc:
                for key, value in doc[field_name].items():

                    # Keys are strings because we may eventually just use the dictionary as kwargs in DJango
                    cleaned_doc[str(key)] = self._clean_doc_value(value)

        # Transpose _id -> id
        cleaned_doc["id"] = doc.get("id", doc.get("_id"))
        cleaned_doc["_score"] = doc["_score"]

        # Compatability with ES < 7.0: Add _type if it exists
        # cleaned_doc["_type"] = doc.get("_type", None)

        return cleaned_doc

    # noinspection PyUnusedLocal
    def type_from_fields(self, fields, parent: "BaseMap" = None):
        # This is the default implementation
        # BoundSuspendersQuerySet will overwrite this dynamically with their own function
        return Result(fields)

    def as_dict(self):
        """Return a list of dictionaries containing the values from the matched set"""
        if not self._object_dictionary:
            objects = []
            for doc in self._documents:
                objects.append(self._clean_doc(doc))

            self._object_dictionary = objects

        return self._object_dictionary

    def as_objects(self):
        """Return list of container objects with attributes from the matched set
        This is the default.
        """
        if not self._objects:
            # t0 = time()
            objects = []
            for doc in self.as_dict():
                obj = self.type_from_fields(doc)
                if obj:
                    objects.append(obj)

            self._objects = objects
            # t1 = time()
            # logger.debug('as_objects: %.3fs.' % (t1 - t0))

        return self._objects
