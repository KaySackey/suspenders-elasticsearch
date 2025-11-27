"""
Example:

class TestType(BaseModel):
    doc_mapper = 'test-mapper'
    indexes = ['test-index']

    parsedText = TextField(store=True, term_vector='with_positions_offsets')
    name = TextField(store=True, term_vector='with_positions_offsets')
    position = IntegerField(store=True)
    uuid = KeywordField()

    def prepare_name(self, name):
        return "I am: %s" % name

site.register(Model, TestType)
"""
import typing

from django.conf import settings
from django.db.models.signals import post_save, pre_delete

from ..exceptions import NotFoundError
from ..suspenders import create_es_connection

if typing.TYPE_CHECKING:
    from suspenders.app.model_indexed import IndexedItem
    from suspenders.mappings import BaseManager, BaseMap

__all__ = ["register", "index"]

registry = {}
bulk_indexing = False
disable_indexing = False

default_connection = create_es_connection(**settings.ELASTIC_SEARCH)


def index(sender, *args, **kwargs):
    if disable_indexing:
        return

    obj = kwargs["instance"]
    ModelClass = sender
    _, __, index_test_function = registry[ModelClass.__name__]

    # Object itself can request not to be indexed
    if not getattr(obj, "_should_index", True):
        return False

    # If we don't have a primary key, we can't index it
    if not obj.pk:
        return False

    # Reload object to clear out any dangling ExpressionNodes
    obj = ModelClass.objects.get(pk=obj.pk)

    # Test indexing via registration function
    if index_test_function is not None and not index_test_function(obj):
        return False

    # Save into index
    manager: "BaseManager" = ModelClass.search.objects
    manager.add(obj, id=None, bulk=bulk_indexing)

    return True


def remove(sender, *args, **kwargs):
    if disable_indexing:
        return

    obj = kwargs["instance"]
    ModelClass = sender

    # Save into index
    try:
        manager: "BaseManager" = ModelClass.search.objects
        manager.delete(id=obj.id, bulk=bulk_indexing)
    except NotFoundError:
        return False

    return True


def register(
    ModelClass: "typing.Type[IndexedItem]",
    MapClass: "typing.Type[BaseMap]",
    index_test_function=None,
):
    """
    index_test_function is a callable that accepts one parameter, the model to be indexed and returns true if the model should be allowed in the index
    """
    registry[ModelClass.__name__] = (ModelClass, MapClass, index_test_function)
    ModelClass.search = MapClass(conn=default_connection)
    post_save.connect(
        index, sender=ModelClass, dispatch_uid="suspenders_index_%s" % ModelClass.__name__
    )
    pre_delete.connect(
        remove, sender=ModelClass, dispatch_uid="suspenders_remove_%s" % ModelClass.__name__
    )


def unregister(ModelClass):
    post_save.disconnect(
        index, sender=ModelClass, dispatch_uid="suspenders_index_%s" % ModelClass.__name__
    )
    pre_delete.disconnect(
        remove, sender=ModelClass, dispatch_uid="suspenders_remove_%s" % ModelClass.__name__
    )
    ModelClass.search = None
    del registry[ModelClass.__name__]
