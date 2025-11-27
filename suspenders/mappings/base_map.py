from django.conf import settings as django_settings

import typing
from elasticsearch import Elasticsearch
from typing import Union

from ..exceptions import IndexMissingException, NotFoundError
from ..query_set import BoundSuspendersQuerySet
from ..utils import handle_elastic_search_errors
from .base_manager import BaseManager
from .fields import SearchField

# Types
Field = Union["SearchField", "BaseMap"]

# noinspection PyProtectedMember
class BaseMapMetaClass(type):
    """
    Basically compiles the Meta class found in each map

    It will merge objects with the last one in the inheritance chain winning in the case of conflicts
    except in the case of the 'fields' attribute.

    _meta.fields will equal all the attributes that are either SearchField or BaseMap
    """

    def __new__(mcs, name, bases, attrs):

        # Handle Meta class creation like Django does
        super_new = super(BaseMapMetaClass, mcs).__new__
        parents = [b for b in bases if isinstance(b, BaseMapMetaClass)]
        if not parents:
            # If this isn't a subclass of BaseMap, don't do anything special.
            return super_new(mcs, name, bases, attrs)

        _meta = mcs.compile_meta(parents, attrs)
        _meta["name"] = name
        attrs["_meta"] = type("Meta", (object,), _meta)

        # Get inherited fields
        attrs["_meta"].fields = mcs.compile_fields(parents, attrs)

        return type.__new__(mcs, name, bases, attrs)

    @classmethod
    def compile_meta(mcs, parents, attrs):
        """Update dictionary containing generalized meta data with respect to inheritance"""
        _meta = {}

        # Update with base class meta, allowing override
        for cls in parents:
            if hasattr(cls, "_meta"):
                _meta.update(cls._meta.__dict__)
            else:
                _meta.update(cls.Meta.__dict__)

        # Update with our meta. Overriding base classes
        if "Meta" in attrs:
            _meta.update(attrs["Meta"].__dict__)

        return _meta

    @classmethod
    def compile_fields(mcs, parents, attrs):
        """Update dictionary containing listing of Search Model Fields with respect to inheritance"""
        fields = {}

        # Update with base class meta, allowing override
        for cls in parents:
            if hasattr(cls, "_meta"):
                fields.update(cls._meta.fields)

        for name, attribute in attrs.items():
            if isinstance(attribute, (SearchField, BaseMap)):
                fields[name] = attribute

        return fields


class DocTypeNotSet:
    pass


class BaseMap(metaclass=BaseMapMetaClass):
    """
    Manage the creation and updating of mapped indexes in ElasticSearch DB

    Usage:
        Definite the properties of the mapping like a django model.

        Example:
        class Example(Mapping):
            uuid = TextField(index='not_analyzed', store='yes')

        - Use create_on(conn) to create the mapping AND index on the db
             This will fail if the index is already created.
        - Use put_on(conn) to put the mapping to the db

        Object Manger is embedded here as a convenience.
    """

    class Meta:
        manager = BaseManager
        settings = {}
        fields: typing.Dict[str, typing.Union[SearchField, "BaseMap"]]

    # Used by BaseManager
    _meta: Meta
    is_suspenders_model = True

    conn: Elasticsearch = None
    objects: "BaseManager"
    doc_type = DocTypeNotSet
    indexes = []
    _properties = None

    def __init__(self, conn=None):
        if self.doc_type is DocTypeNotSet:
            # Default doc type is the lower cased class name
            self.doc_type = self.__class__.__name__.lower()

        if not self.indexes and self.doc_type:
            # Default index is the doc type
            self.indexes = [self.doc_type]

        self.objects = self._meta.manager(map=self, conn=conn)

        # Add prefix to indices
        prefix = getattr(django_settings, "ELASTIC_SEARCH_PREFIX", "")
        self.indexes = [f"{prefix}{i}" for i in self.indexes]

        if conn:
            self.set_connection(conn)

    @property
    def primary_index(self):
        return self.indexes[0]

    def set_connection(self, conn):
        self.conn = conn
        self.objects.set_connection(conn)

    def prepare(self, obj, document):
        """Modify the document as a whole before it is added to the index
        Subclasses should override this method.

        Additionally, methods in the form prepare_N
        where N is the name of an attribute can be used to prepare individual attributes before indexing.
        """
        return document

    def full_indexes(self):
        """
        Total list of indexes
        """
        return self.indexes

    def create(self, delete_first=False):
        """Create the mapping for type on indexes"""
        return self.put()

    def put_settings(self, settings=None):
        """
        Try to add settings to indexes.
        Return a list with the names of indexes which could not be updated
        """
        if settings is None:
            settings = self._meta.settings

        self.conn.indices.put_settings(index=self.primary_index, body=settings)

    def get_settings(self):
        """
        Try to add settings to indexes.
        Return a list with the names of indexes which could not be updated
        """
        return self.conn.indices.get_settings(index=self.primary_index)

    def optimize(self):
        """Try to optimize indexes"""
        self.conn.indices.forcemerge(index=self.primary_index)

    def create_indexes(self, delete_first=False):
        """Try to create indexes.
        Return a list with the names of indexes which could not be created
        """
        if delete_first:
            self.delete_indexes()

        body = {
            "settings": self._meta.settings,
            "mappings": self.to_json()
            # include_type_name
        }

        self.conn.indices.create(index=self.primary_index, body=body)

    def query_set(self) -> BoundSuspendersQuerySet:
        return BoundSuspendersQuerySet(
            conn=self.conn, indexes=self.indexes, callback=self.execute_callback
        )

    def delete_indexes(self):
        """Try to delete indexes.
        Return a list with the names of indexes which could not be deleted
        """
        try:
            with handle_elastic_search_errors(body=f"DELETE {self.primary_index}"):
                self.conn.indices.delete(index=self.primary_index)
        except IndexMissingException:
            pass

    def put(self):
        """ Put the mapping on indexes"""
        for index in self.full_indexes():
            self.conn.indices.put_mapping(body=self.to_json(), index=index)

    def delete_mapping(self):
        """ Delete mapping from indexes""" ""
        for index in self.full_indexes():
            try:
                self.conn.indices.delete_mapping(index)
            except NotFoundError:
                pass

    def refresh_indexes(self):
        self.conn.indices.refresh(self.indexes)

    def flush_indexes(self, wait_if_ongoing=True):
        self.conn.indices.flush(self.primary_index, wait_if_ongoing=wait_if_ongoing)

    def execute_callback(self, result_set):
        return result_set

    @property
    def properties(self):
        """ Return a dictionary of properties """
        return self._meta.fields

    def to_json(self):
        """ Return the JSON representation of this mapping"""
        properties = {}

        for key, value in self.properties.items():
            # Todo: Handle BaseMap types as ObjectField
            # This will enable us to get rid of things
            # like searching on created_by_id instead of created_by.id referincing the inner user field

            # Right now they're just splatted out as a JSON with dynamic field typing
            # https://www.elastic.co/guide/en/elasticsearch/reference/current/object.html
            # Using an ObjectType will allow us to query subfields
            if not isinstance(value, BaseMap):
                properties[key] = value.to_json()

        return {"properties": properties}
