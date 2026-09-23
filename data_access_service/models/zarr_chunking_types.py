from dataclasses import dataclass


@dataclass(frozen=True)
class ZarrChunkingConfig:
    """How the zarr subset job sizes dask time chunks (see
    batch/subsetting/helpers/zarr_chunking.py), read from the
    ``subsetting:`` section of config.yaml.
    """

    # Leave this many GB unused so zlib/dask copies stay under the
    # container limit (8GB Fargate → 6GB peak).
    headroom_gb: float
    # On larger hosts, cap peak at this fraction of total RAM instead.
    target_peak_fraction: float
    min_chunk_mb: int
    # Fraction of the remaining peak budget used for one chunk.
    memory_fraction: float
    # Hard ceiling on process RSS. A time block is sized from the room left
    # under this, so it is not added on top of memory already in use.
    max_chunk_gb: float

    @property
    def headroom_bytes(self) -> int:
        return int(self.headroom_gb * 1024**3)

    @property
    def min_chunk_bytes(self) -> int:
        return int(self.min_chunk_mb * 1024**2)

    @property
    def max_chunk_bytes(self) -> int:
        return int(self.max_chunk_gb * 1024**3)
