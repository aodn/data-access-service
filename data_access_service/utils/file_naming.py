"""Names for the files the user downloads.

The user gets a plain S3 url in the result email, so the s3 key's last segment
*is* the file name they see. Name it after the collection, not the internal
dataset key, and add the dataset only when the collection has more than one
(otherwise two datasets would end up with the same file name).
"""

import re
from typing import Optional

# Anything outside this set is replaced: a "/" would make a fake s3 folder and
# a space would break the raw <a href> in the email.
_UNSAFE_CHARS = re.compile(r"[^A-Za-z0-9._-]+")
_MAX_BASE_NAME_LENGTH = 150

KNOWN_KEY_SUFFIXES_TO_STRIP = (".parquet", ".zarr")


def sanitise_for_filename(text: str) -> str:
    """Turn free text into a safe file name part. Empty when nothing is left."""
    cleaned = _UNSAFE_CHARS.sub("_", text or "").strip("_.-")
    return cleaned[:_MAX_BASE_NAME_LENGTH].strip("_.-")


def dataset_base_name(key: str) -> str:
    """The dataset key without its storage suffix, e.g. "foo.zarr" -> "foo"."""
    for suffix in KNOWN_KEY_SUFFIXES_TO_STRIP:
        if key.endswith(suffix):
            return key[: -len(suffix)]
    return key


def build_download_base_name(
    collection_title: Optional[str],
    key: str,
    has_multiple_datasets: bool,
) -> str:
    """File name (no extension) for the download of `key`.

    {collection}, or {collection}-{dataset} when the collection holds more than
    one dataset. Falls back to the dataset key when the title is missing or is
    made up entirely of unsafe characters.
    """
    dataset = dataset_base_name(key)
    collection = sanitise_for_filename(collection_title)
    if not collection:
        return sanitise_for_filename(dataset) or dataset

    if not has_multiple_datasets:
        return collection

    suffix = sanitise_for_filename(dataset)
    return f"{collection}-{suffix}" if suffix else collection
