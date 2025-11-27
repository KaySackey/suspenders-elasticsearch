from elasticsearch import JSONSerializer


class SetEncoder(JSONSerializer):
    """
    Extends serializer to include sets
    """

    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return JSONSerializer.default(self, obj)
