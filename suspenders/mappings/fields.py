class SearchField:
    def __init__(
        self,
        # Model attribute to pull from
        model_attr=None,
        # If true, then when we find a null value
        # we will not replace it with a deferred
        nullable=False,
        **kwargs
    ):
        """
        Defines a ES mapping type.
         kwargs will be stored as is and given to the handler

        :param model_attr: Used to pull the data from the model
        :param kwargs:
        """
        self.model_attr = model_attr
        self.nullable = nullable
        self.kwargs = kwargs

    def to_json(self):
        return self.kwargs


class TextField(SearchField):
    def __init__(self, **kwargs):
        super().__init__(type="text", **kwargs)


class KeywordField(SearchField):
    """
    A string field which is not analyzed
    """

    def __init__(self, **kwargs):
        super().__init__(type="keyword", **kwargs)


class AggregationField(KeywordField):
    """
    Keyword field that is used widely in aggregations
    eager_global_ordinals is set to True
    """

    def __init__(self, **kwargs):
        super().__init__(type="keyword", eager_global_ordinals=True, fielddata=True, **kwargs)


class IntegerField(SearchField):
    def __init__(self, **kwargs):
        super().__init__(type="integer", **kwargs)
        self.type = "integer"


class DoubleField(SearchField):
    """
    Python Floats are doubles so we provide this
    """

    def __init__(self, **kwargs):
        super().__init__(type="double", **kwargs)
        self.type = "double"


class DateField(SearchField):
    def __init__(self, **kwargs):
        super().__init__(type="date", **kwargs)


class BooleanField(SearchField):
    def __init__(self, **kwargs):
        super().__init__(type="boolean", **kwargs)
