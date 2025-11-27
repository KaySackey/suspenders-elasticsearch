from ..lib import MapperCommand


class Command(MapperCommand):
    help = "Place settings into indexes using a registered mapper"
    message_prefix = "Settings"

    def handle_map(self, map, ModelClass, *args, **options):
        map.put_settings()
        self.info("Added settings for %s" % map._meta.name)
