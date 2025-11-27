import logging

from django.conf import settings
from django.db.models import DEFERRED, Model as DbModel
from funcy import cached_property

from .utils import convert_str_to_datetime
from ..mappings import BaseMap
from ..mappings.fields import DateField, IntegerField, KeywordField, SearchField

logger = logging.getLogger("suspenders")


class SuspendersModel(BaseMap):
    """
    Todo:

    Document
    - post_object_creation
    - _get_basic_model
    - _create_model
    """

    class Meta:
        model = None

    id = IntegerField()
    document_type = KeywordField()

    def prepare_document_type(self, obj):
        return self.doc_type

    @cached_property
    def complex_field_names(self):
        return {key for (key, field) in self._meta.fields.items() if isinstance(field, BaseMap)}

    @cached_property
    def nullable_fields(self):
        return {
            key
            for (key, field) in self._meta.fields.items()
            if isinstance(field, SearchField) and field.nullable
        }

    @cached_property
    def datetime_fields(self):
        return [key for (key, field) in self._meta.fields.items() if isinstance(field, DateField)]

    def execute_callback(self, result_set):
        """Must return the result set
        Inject our type from fields into the result set so .as_objects() will return the proper types
        """
        result_set.type_from_fields = self.type_from_fields
        return result_set

    def process_load_fields(self, load_fields, base, parent):
        """
        Process load_fields before you create an object
        """
        load_fields["id"] = int(load_fields["id"])

        for key in self.datetime_fields:
            load_fields[key] = convert_str_to_datetime(load_fields.get(key, None))

        # Check if we need to add a None
        # because a field could be null
        for field_name in self.nullable_fields:
            if field_name not in load_fields:
                load_fields[field_name] = None

        return load_fields

    def _model_class_key(self, load_fields, base, parent):
        """
        Return the cache key for this model class. Paired with _create_model
        """
        return str(self.__class__) + str(load_fields.keys())

    def post_object_creation(self, obj, processed, base, parent):
        """
        Subclass hook - do something special now that we have an object
        """
        return obj

    def _get_basic_model(self, base, parent):
        """
        Return the basic model without any special treatment to it
        This can be overwritten in subclasses to return different models depending on attributes in the base/parent
        """
        return self._meta.model

    def type_from_fields(self, base, parent=None):
        """
        Return a instantiated Model class
        """
        # Determine which of the elements in the returned search correspond to a 'load field' and a 'complex field'
        # A load field iso ne that is stored inline in the relational database
        # Complex fields are database relations (model objects in Django, model maps in Suspenders)
        load_fields = dict((k, v) for (k, v) in base.items() if k not in self.complex_field_names)

        # Default processor will ensure id is always an integer
        load_fields = self.process_load_fields(load_fields, base, parent)

        if "id" not in load_fields:
            raise TypeError("id not present in load_fields. Value of load_fields: %s" % load_fields)

        ModelClass = self._get_basic_model(base, parent)
        obj = ModelClass(
            **{
                f.attname: load_fields.get(f.attname, DEFERRED)
                for f in ModelClass._meta.concrete_fields
            }
        )

        # Reify all AbstractModels
        # search_results will end up containing tree of all attributes
        # including those complex attributes which will have been converted from their raw form

        # Fixup foreign keys which are represented as AbstractModels
        processed_result_map = base.copy()
        foreign_key_field_names = {
            getattr(f, "name", getattr(f, "attname", None))
            for f in ModelClass._meta.get_fields()
            if f.is_relation
        }

        for field_name in self.complex_field_names:
            # Optimization
            # This changes the previous semantic of always unpacking
            # Skip instances where the field isnt' a base field anys
            exists_in_data = field_name in base
            if not exists_in_data:
                continue

            field = getattr(self, field_name, None)
            # can_be_unpacked = isinstance(field, BaseMap)

            try:
                # Add it to the processed map of results
                raw_values = base[field_name]
                if isinstance(raw_values, (set, list)):
                    rel = [field.type_from_fields(value, base) for value in raw_values]
                elif raw_values is None:
                    # A complex field can resolve to None if this value was added to the document
                    # rather than simply being missing from the dict
                    rel = None
                else:
                    rel = field.type_from_fields(raw_values, base)

                processed_result_map[field_name] = rel

                is_relation = field_name in foreign_key_field_names
                if is_relation:
                    setattr(obj, field_name, rel)
            except (AttributeError, TypeError):
                # This can fail because type_from_fields is not defined
                if settings.DEBUG:
                    raise
                else:
                    logger.exception(f"Could not decode {field_name}")

        # Embed original results
        obj.search_results = processed_result_map
        obj = self.post_object_creation(obj, processed_result_map, base, parent)

        if not obj:
            raise ValueError("Object wasn't returned from post_object_creation")

        return obj
