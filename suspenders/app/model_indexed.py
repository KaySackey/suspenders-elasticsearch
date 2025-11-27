import logging
import typing
from typing import Any, ClassVar, Dict

from django.conf import settings
from django.utils.functional import cached_property

from suspenders.app import sites
from .models import SuspendersModel

logger = logging.getLogger("suspenders")

if typing.TYPE_CHECKING:
    from ..mappings import BaseManager
    from .models import SuspendersModel


class NotThere:
    pass


def _get_search_results(obj: "IndexedItem", name: str):
    if not hasattr(obj, "search_results"):
        return False, None

    value = obj.search_results.get(name, NotThere)

    if value is NotThere:
        return False, None

    return True, value


class IndexedItem:
    class Meta:
        abstract = True

    id: int
    search: ClassVar["SuspendersModel"] = None
    search_results: Dict[str, Any]

    _should_index = True

    def get_search_results(self, name: str, default=None) -> Any:
        """
        Check the search result dictionary to see if an attribute is listed.
        If it is listed, then return it otherwise use the default parameter as the return
        """
        value_exists, value = _get_search_results(self, name)

        if value_exists:
            return value

        if hasattr(default, "__call__"):
            return default()

        return default


    if getattr(settings, "DEBUG_REFRESHES", False):
        # Do not define this in production
        # because it causes a bug where if you *ever* ask for a
        # non-existent property on a model (e.g. Album._NOPE__)
        # then all the descriptors on the model are reset to their unbound versions
        def refresh_from_db(self, using=None, fields=None):
            # We must inherit from a django model
            if hasattr(super(), 'refresh_from_db'):
                import logging
                logging.exception(f"Refreshing DB for {fields} in {self.__class__.__name__}")
                import traceback
                traceback.print_stack()
                super().refresh_from_db(using, fields)

    def add_to_index(self, *, bulk=False):
        if not hasattr(self.__class__, "search"):
            raise NotImplementedError
        if sites.disable_indexing:
            if not settings.DEBUG:
                raise ValueError("Tried to index something without indexing enabled")

            return

        manager: "BaseManager" = self.__class__.search.objects

        try:
            return manager.add(self, bulk=bulk)
        except TypeError as exc:
            # Error is "Object of type CombinedExpression is not JSON serializable"
            # Due to somewhere up stream using a Django F-function to update the model
            if "CombinedExpression" in str(exc):
                super().refresh_from_db()
                return manager.add(self)

            # Re-raise error if it isn't what we expect
            raise

    def remove_from_index(self, *, bulk=False):
        if not hasattr(self.__class__, "search"):
            raise NotImplementedError
        if sites.disable_indexing:
            if not settings.DEBUG:
                raise ValueError("Tried to index something without indexing enabled")
            return

        manager: "BaseManager" = self.__class__.search.objects

        try:
            return manager.delete(id=self.id, bulk=bulk)
        except:
            pass

    def save_without_indexing(
        self, force_insert=False, force_update=False, using=None, update_fields=None
    ):
        """
        Django specific.

        Update a field without changing its status in the index
        Requires this class be attached to a Django model
        """
        self._should_index = False

        # noinspection PyUnresolvedReferences
        self.save(force_insert, force_update, using, update_fields)

        self._should_index = True


class search_property(cached_property):
    """
    Check search results before calling function
    """

    name = None

    def __get__(self, instance, cls=None):
        """
        Call the function and put the return value in instance.__dict__ so that
        subsequent attribute access on the instance returns the cached value
        instead of calling cached_property.__get__().
        """
        if instance is None:
            return self

        value_exists, value = _get_search_results(instance, self.name)

        if value_exists:
            return value

        res = instance.__dict__[self.name] = self.func(instance)
        return res
