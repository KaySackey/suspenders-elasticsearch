from ..lib import MapperCommand
from .create_mapping import Command as CreateMappingCommand
from .delete_indexes import Command as DeleteIndexesCommand
from .populate_indexes import Command as PopulateIndexesCommand


class Command(MapperCommand):
    help = "Run delete_mapping, create_mapping, and populate_indexes in sync"

    def handle_map(self, map, ModelClass, *args, **options):
        # Pipeline commands
        pipeline = [DeleteIndexesCommand(), CreateMappingCommand(), PopulateIndexesCommand()]

        for command in pipeline:
            command.stdout = self.stdout
            command.verbosity = self.verbosity
            command.handle_map(map, ModelClass, *args, **options)

        self.info("Finished Rebuilding indexes")
