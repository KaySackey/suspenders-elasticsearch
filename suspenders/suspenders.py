import logging
from elasticsearch import Elasticsearch

from .serializer import SetEncoder

logger = logging.getLogger("suspenders")


def create_es_connection(server, timeout=5.0, max_retries=1, http_auth=None):
    if not server:
        logging.exception("No server given!")
        return

    hosts = [
        {
            "host": host,
            "port": port,
        }
        for (host, port) in server
    ]

    return Elasticsearch(
        hosts=hosts,
        timeout=timeout,
        max_retries=max_retries,
        http_auth=http_auth,
        retry_time=timeout,
        http_compress=True,
        serializer=SetEncoder(),
    )
