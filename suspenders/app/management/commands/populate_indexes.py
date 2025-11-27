from django.conf import settings
from django.core.paginator import Paginator

import typing

from ..lib import MapperCommand

if typing.TYPE_CHECKING:
    from suspenders.mappings import BaseMap


class Command(MapperCommand):
    help = "Populate indexes with objects from database, using a registered mapper"
    message_prefix = "Populate"

    def handle_map(self, map, ModelClass, *args, **options):
        start = options["start"]
        end = options["end"]

        query_set = ModelClass.objects.all().order_by("-id")

        select_related_fields = getattr(map._meta, "select_related", [])

        if select_related_fields:
            query_set = query_set.select_related(*select_related_fields)

        if hasattr(ModelClass, "visible"):
            query_set = query_set.filter(visible=True)

        if hasattr(map._meta, "prepare_bulk_query_set"):
            query_set = map._meta().prepare_bulk_query_set(query_set)

        if start:
            query_set = query_set.filter(id__lt=int(start))
        if end:
            query_set = query_set.filter(id__gt=int(end))

        self.process(map, query_set, int(options["chunk_size"]))

    def process(self, map: "BaseMap", query_set, chunk_size=100):
        """
        Take a query set and add those objects to ElasticSearch
        """

        map.put_settings({"index": {"refresh_interval": "-1"}})

        try:
            num = 0
            total = query_set.count()

            self.out(2, f"Found {total} items")
            paginator = Paginator(query_set, chunk_size)

            for x in paginator.page_range:
                page = paginator.page(x)

                for model in page.object_list:
                    num += 1
                    title = str(model)
                    try:
                        map.objects.add(model, bulk=True)
                        self.log(
                            "Added item %s of %s - [id: %s]: %s" % (num, total, model.id, title)
                        )
                    except:
                        self.error(
                            "Error on item %s of %s - [id: %s]: %s" % (num, total, model.id, title)
                        )
                        if settings.DEBUG:
                            raise

                map.objects.flush_bulk()

            self.info("Added all items for %s" % map._meta.name)
        finally:
            map.refresh_indexes()
            map.flush_indexes()
            map.put_settings({"index": {"refresh_interval": "1s"}})
            # map.optimize()
