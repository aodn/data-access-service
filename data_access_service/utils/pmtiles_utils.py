import requests

from pmtiles.reader import Reader
from contextlib import contextmanager


@contextmanager
def open_pmtiles(path):
    f = open(path, "rb")

    def get_bytes(offset, length):
        f.seek(offset)
        return f.read(length)

    try:
        yield Reader(get_bytes)
    finally:
        f.close()


@contextmanager
def open_pmtiles_http(url):
    session = requests.Session()

    def get_bytes(offset, length):
        headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
        r = session.get(url, headers=headers)
        r.raise_for_status()
        return r.content

    try:
        yield Reader(get_bytes)
    finally:
        session.close()


def is_local_pmtiles_valid(remote_url: str, file_path: str) -> bool:
    """
    By default, data is increasing. Therefore, if local new generated pmtiles has less data than remote one,
    There may be something wrong with local one. So the local one is considered invalid and let developers to doublecheck.
    """

    def get_layer_counts(metadata):
        if "tilestats" not in metadata or "layers" not in metadata["tilestats"]:
            return None

        return {
            layer["layer"]: layer["count"] for layer in metadata["tilestats"]["layers"]
        }

    with open_pmtiles_http(remote_url) as reader:
        remote_metadata = reader.metadata()

    with open_pmtiles(file_path) as reader:
        local_metadata = reader.metadata()

    remote_counts = get_layer_counts(remote_metadata)
    local_counts = get_layer_counts(local_metadata)

    if local_counts is None or remote_counts is None:
        return False

    if set(remote_counts.keys()) != set(local_counts.keys()):
        return False

    for layer_name in remote_counts:
        if remote_counts[layer_name] > local_counts[layer_name]:
            return False

    return True
