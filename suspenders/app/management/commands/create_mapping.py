from ..lib import MapperCommand


class Command(MapperCommand):
    help = "Place static mappings to indexes using a registered mapper"
    message_prefix = "Create"

    def handle_map(self, map, ModelClass, *args, **options):
        map.create_indexes(delete_first=False)
        self.info("Created indexes for %s" % map._meta.name)
