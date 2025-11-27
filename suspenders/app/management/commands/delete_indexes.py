from ..lib import MapperCommand


class Command(MapperCommand):
    help = "Rebuild an index under a registered mapper."
    message_prefix = "Delete"

    def handle_map(self, map, ModelClass, *args, **options):
        map.delete_indexes()
        self.info("Deleted indexes for %s" % map._meta.name)
