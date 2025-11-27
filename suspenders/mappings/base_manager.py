import typing
from elasticsearch import Elasticsearch

from .bulk_manager import BulkManager

if typing.TYPE_CHECKING:
    from suspenders.mappings import BaseMap


class BaseManager:
    """
     Manage the retrieval/creation of documents on ElasticSearch DB
    Depends on the map class to determine how the document is laid out.
    """

    map: "BaseMap"
    bulker: BulkManager

    def __init__(self, map: "BaseMap", conn: Elasticsearch):
        self.map = map
        self.conn = conn

        self.indexes = map.indexes

        # Setup bulker.
        # We may want make this a singleton in the future
        self.bulker = BulkManager(self.conn)

    def set_connection(self, conn):
        self.bulker.conn = conn
        self.conn = conn

    def get(self, id):
        """Get a typed JSON document from an index based on its id."""
        index = self.map.primary_index
        return self.conn.get(index=index, id=id)

    def add(self, obj, *, id=None, bulk=False):
        """Add object to indexes"""

        # Determine the ID of the object
        id = self._get_id(obj, id)
        doc = self._object_as_dict(obj)
        index = self.map.primary_index

        if bulk:
            self.bulker.index(id=id, doc=doc, index=index)
        else:
            self.conn.index(body=doc, index=index, id=id)

    def flush_bulk(self):
        self.bulker.flush()

    def delete(self, *, id=None, bulk=False):
        """Delete a specific object"""

        if not id:
            raise ValueError("ID must be specified")

        index = self.map.primary_index

        if bulk:
            self.bulker.delete(id=id, index=index)
        else:
            self.conn.delete(index=self.map.primary_index, id=id)

    #
    # Below are all used to pull data from an object, and prepare it according to the current Map
    #
    def _get_id(self, obj, id):
        if id is None:
            id = self._get_id_from_obj(obj, raise_errors=False)
        elif type(id) is not int:
            raise ValueError("Id must be an integer. id%s = %s" % (type(id), id))

        return id

    def _get_id_from_obj(self, obj, raise_errors=False):
        try:
            retr = self._retr_method_for(obj)
            pk = retr("id")
        except (KeyError, AttributeError, TypeError):
            pk = None

            if raise_errors:
                raise AttributeError("Id attribute not found in object: %s" % obj)

        return pk

    def _serialize_field_value(self, retr, name, field):
        """
        Given a field, use it to get a serialize a value
        """
        if getattr(field, "is_suspenders_model", False):
            # Handle embedded models
            retr_name = name
            inner_obj = retr(retr_name)

            if inner_obj is None:
                # If the embedded object is not present (e.g. because the field is nullable, etc.)
                # then do nothing
                return None

            # Lists of objects have to be reified manually
            if isinstance(inner_obj, (set, list)):
                return [field.objects._object_as_dict(inner_item) for inner_item in inner_obj]
            else:
                return field.objects._object_as_dict(inner_obj)
        else:
            retr_name = name if field.model_attr is None else field.model_attr
            return retr(retr_name)

    def _object_as_dict(self, obj):
        retr = self._retr_method_for(obj)

        document = {}
        items = self.map._meta.fields.items()
        for name, field in items:
            serialized_value = self._serialize_field_value(retr, name, field)
            if serialized_value is not None:
                document[name] = serialized_value

        document = self.map.prepare(obj, document)
        return document

    def _retr_method_for(self, obj):
        if isinstance(obj, dict):
            retrieval_method = BaseManager._retr_from_dict
        else:
            retrieval_method = BaseManager._retr_from_object

        def prepare_attribute(name):
            """Prepare attribute value, using prepare_<name> function found on the associated Map/Model"""
            prepare_N = "prepare_" + name
            if hasattr(self.map, prepare_N):
                method = getattr(self.map, prepare_N)
                value = method(obj)
            else:
                value = retrieval_method(obj, name)

            return value

        return prepare_attribute

    #
    # Retrieval methods for attributes within the object
    # These are listed returned as named functions (as opposed to anonymous functions, etc.) for speed
    #
    @staticmethod
    def _retr_from_dict(obj, key):
        keys = key.split("__")
        value = obj

        for k_at_depth in keys:
            value = value[k_at_depth]

        return value

    @staticmethod
    def _retr_from_object(obj, key):
        keys = key.split("__")
        value = obj

        def value_from_object(o, name):
            attr = getattr(o, name)

            if hasattr(attr, "__call__"):
                return attr()
            else:
                return attr

        for k_at_depth in keys:
            value = value_from_object(value, k_at_depth)

        return value
