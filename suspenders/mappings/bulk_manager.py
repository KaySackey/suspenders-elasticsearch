# parallel_bulk
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk as bulk_helper


class BulkManager:
    conn: Elasticsearch
    actions = []
    chunk_size: int

    def __init__(self, conn, chunk_size=500):
        self.conn = conn
        self.chunk_size = chunk_size

    def index(self, id: int, doc: dict, index: str):
        """Add object to indexes"""
        self.append_action({"_op_type": "index", "_index": index, "_id": id, "_source": doc})

    def delete(self, id: int, index: str):
        """Delete a specific object"""
        self.append_action({"_op_type": "delete", "_index": index, "_id": id})

    def append_action(self, action: dict):
        self.actions.append(action)

        if len(action) >= self.chunk_size:
            self.flush()

    def flush(self):
        # Run actions in bulk
        actions = self.actions

        bulk_helper(client=self.conn, actions=actions, chunk_size=self.chunk_size)

        # cleanup locals
        self.actions = []
