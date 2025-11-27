from django.conf import settings

from apps.app_helpers.lib.management import Command as BaseCommand
# Suspenders Search
from ...app.sites import registry
from ...suspenders import create_es_connection


class MapperCommand(BaseCommand):
    """ A command to be run in a registered mapper """

    help = "<mapper_1, mapper_2, ..., mapper_N> to define a list of mappers or --all to run command on all registered mappers"
    can_import_settings = True

    def add_arguments(self, parser):
        parser.add_argument(
            "indexes",
            type=str,
            nargs="+",
            help='A list of index names. Check with --list to find out what these names are or use "all" to rebuild all indexes',
        )
        parser.add_argument(
            "--list",
            action="store_true",
            dest="list",
            default=False,
            help="List registered mappers",
        )
        parser.add_argument(
            "--start",
            action="store",
            dest="start",
            default=None,
            help="Only use items > this ID. Best used when populating a single index.",
        )
        parser.add_argument(
            "--end",
            action="store",
            dest="end",
            default=None,
            help="Only use items < this ID. Best used when populating a single index.",
        )
        parser.add_argument(
            "--chunk_size",
            action="store",
            dest="chunk_size",
            default=100,
            help="Size of chunk to process on ES",
        )

    def handle(self, *args, **options):
        indexes = options["indexes"]

        if options["list"]:
            self.handle_list()

        if "all" in indexes:
            indexes = registry.keys()

        # Extend connection timeout
        es_settings = {**settings.ELASTIC_SEARCH, "timeout": 30.0}
        connection = create_es_connection(**es_settings)

        for map_name in indexes:
            # MapClass: SuspendersModel.__class__

            try:
                ModelClass, MapClass, _indexer = registry[map_name]
            except KeyError:
                return self.info("No such mapper %s" % map_name)

            map = MapClass(conn=connection)
            self.handle_map(map, ModelClass, *args, **options)

        self.info("Done\n")

    def handle_list(self):
        for key in registry.keys():
            self.stdout.write("\t" + key + "\n")

        raise SystemExit

    def handle_map(self, map, ModelClass, *args, **options):
        raise NotImplementedError
