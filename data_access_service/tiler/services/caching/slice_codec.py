"""Encode a slice's CSR grids for the L1 cache without copying on the way back.
"""

import json
import struct
from typing import Any

import numpy as np

from data_access_service.tiler.services.store.sparse_grid import SparseGrid

# collections.abc.Buffer is 3.12+; this project still builds on 3.11.
Buffer = bytes | bytearray | memoryview

_HEADER_LEN = struct.Struct("<I")
_ALIGN = 8
_FIELDS = ("row_ptr", "j", "value")


def _pad(n: int) -> int:
    """``n`` rounded up to the alignment."""
    return (n + _ALIGN - 1) // _ALIGN * _ALIGN


def encode(grids: dict[str, SparseGrid]) -> bytes:
    """``grids`` as one buffer: header, then every array back to back."""
    layout: dict[str, Any] = {}
    arrays: list[tuple[int, np.ndarray]] = []
    offset = 0
    for name, grid in grids.items():
        fields = {}
        for field in _FIELDS:
            arr = np.ascontiguousarray(getattr(grid, field))
            offset = _pad(offset)
            fields[field] = [arr.dtype.str, int(arr.size), offset]
            arrays.append((offset, arr))
            offset += arr.nbytes
        layout[name] = {"n_i": grid.n_i, "n_j": grid.n_j, "fields": fields}

    header = json.dumps({"grids": layout}, separators=(",", ":")).encode()
    data_start = _pad(_HEADER_LEN.size + len(header))

    out = bytearray(data_start + offset)
    out[: _HEADER_LEN.size] = _HEADER_LEN.pack(len(header))
    out[_HEADER_LEN.size : _HEADER_LEN.size + len(header)] = header
    view = memoryview(out)
    for at, arr in arrays:
        start = data_start + at
        view[start : start + arr.nbytes] = arr.view(np.uint8).reshape(-1)
    return bytes(out)


def decode(blob: Buffer) -> dict[str, SparseGrid]:
    """The grids ``encode`` wrote, as views onto ``blob`` - no array is copied.

    ``blob`` can be any buffer: ``bytes`` from a plain GET, or a read-only
    memoryview over the buffer a chunked read filled.
    """
    (header_len,) = _HEADER_LEN.unpack_from(blob, 0)
    header_at = _HEADER_LEN.size
    # bytes() because json.loads rejects a memoryview. Only the header, not the
    # arrays, is copied here.
    header = bytes(memoryview(blob)[header_at : header_at + header_len])
    layout = json.loads(header)["grids"]
    data_start = _pad(header_at + header_len)

    grids: dict[str, SparseGrid] = {}
    for name, spec in layout.items():
        parts = {}
        for field in _FIELDS:
            dtype, count, offset = spec["fields"][field]
            parts[field] = np.frombuffer(
                blob, dtype=np.dtype(dtype), count=count, offset=data_start + offset
            )
        grids[name] = SparseGrid(
            spec["n_i"], spec["n_j"], parts["row_ptr"], parts["j"], parts["value"]
        )
    return grids
