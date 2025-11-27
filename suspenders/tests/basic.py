from ..mappings import BaseMap
from ..mappings.fields import IntegerField, KeywordField, TextField
from ..suspenders import create_es_connection

# Requires ElasticSearch to be running on localhost:9200
es = create_es_connection("127.0.0.1:9200")

# Todo: Update Tests.
# These tests relied on https://github.com/syrusakbary/snapshottest
# They worked, but you'll just have to take my word for it.

class Model(BaseMap):
    """
    This helps us test it if inheritance works properly
    """

    id = IntegerField()


class TestType(Model):
    doc_type = "test-type"
    indexes = ["test-index"]

    parsedText = TextField(store=True, term_vector="with_positions_offsets")
    name = TextField(store=True, term_vector="with_positions_offsets")
    position = IntegerField(store=True)
    uuid = KeywordField()
    tags = TextField()

    def prepare_tags(self, object):
        if "tags" in object:
            return object["tags"]
        else:
            return []

    def prepare_name(self, object):
        return "I am: %s" % object["name"]


documents = [
    {"name": "AAA Tester", "parsedText": "", "uuid": "1", "position": 1},
    {"name": "BBB Baloney", "parsedText": "", "uuid": "1", "position": 1},
    {
        "name": "CCC Baloney",
        "parsedText": "",
        "uuid": "2",
        "position": 2,
        "tags": ["hello", "universe"],
    },
    {"name": "DDD Baloney", "parsedText": "", "uuid": "2", "position": 2},
    {"name": "EEE Baloney", "parsedText": "", "uuid": "3", "position": 3},
    {
        "name": "FFF Baloney",
        "parsedText": "",
        "uuid": "3",
        "position": 4,
        "tags": ["hello", "world"],
    },
]


def test_map_creation(new_map, snapshot):
    new_map.create(delete=False)

    # Settings from ElasticSearch
    snapshot(new_map.get_settings())
    # settings from json output
    snapshot(new_map.to_json())


def test_document_indexing(es):
    map = TestType(conn=es)
    map.create_indexes()

    # Save into index
    for doc in documents:
        map.objects.add(obj=doc)


def test_document_indexing_bulk(es):
    map = TestType(conn=es)
    map.create_indexes()

    # Save into index
    for doc in documents:
        map.objects.add(obj=doc, bulk=True)

    # Flush bulk changes
    map.objects.flush_bulk()


def test_creation_then_deletion(es):
    map = TestType(conn=es)
    map.create_indexes()
    map.delete_indexes()


def test_put_settings(es):
    map = TestType(conn=es)
    map.create_indexes()
    map.put_settings({"index": {"refresh_interval": "-1"}})


def test_document_serialization(snapshot):
    serialized = [TestType().prepare(doc) for doc in documents]
    snapshot(serialized)
