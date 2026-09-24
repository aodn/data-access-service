"""The tiler batch job: convert every product's zarr store to parquet and
publish each one to ``root_metadata.json`` as it finishes.

How a run goes:

1. Stores run one at a time, by name, each in a forked worker (or
   in-process when ``use_fork_process`` is off), so only one is in memory.
2. In a store, only timestamps not yet converted are read, one zarr time
   chunk at a time, newest chunk first.
3. Each timestamp and variable becomes one parquet file. All-NaN
   timestamps are recorded as empty.
4. The store's ``metadata.json`` is saved after each chunk, so a failed
   run resumes where it stopped.
5. Once a store is done, it is published to ``root_metadata.json``.
"""

import os
from datetime import datetime, timezone

from data_access_service import Config, init_log
from data_access_service.batch.tiler import storage
from data_access_service.batch.tiler.discovery import discover_products
from data_access_service.batch.tiler.parquet_generator import read_metadata, sync_store
from data_access_service.batch.tiler.zarr_registry import close_store, open_store
from data_access_service.core.api import API
from data_access_service.models.tiler_parquet_types import (
    ROOT_METADATA_VERSION,
    ProductIdentity,
    RootMetadata,
    root_metadata_path,
)
from data_access_service.models.tiler_types import TilerBatchConfig
from data_access_service.utils.memory_utils import log_memory_usage
from data_access_service.utils.s3_json import read_json

config = Config.get_config()
logger = init_log(config)


def _group_by_store(
    products: dict[str, ProductIdentity],
) -> dict[str, tuple[str, list[str]]]:
    """``store -> (uuid, sorted variables)`` across all its products, so each
    store is converted once."""
    grouped: dict[str, tuple[str, set[str]]] = {}
    for product in products.values():
        store_uuid, variables = grouped.get(
            product.store, (product.metadata_uuid, set())
        )
        variables.update(product.variables)
        grouped[product.store] = (store_uuid, variables)
    return {store: (uuid, sorted(vars_)) for store, (uuid, vars_) in grouped.items()}


def generate_tiler_parquet_for_all_products(api: API, uuid: str | None = None) -> None:
    """Convert every product's store, updating ``root_metadata.json`` after
    each one. ``uuid`` limits the run to one metadata record."""
    products = discover_products(api)

    if uuid is not None:
        products = {pid: p for pid, p in products.items() if p.metadata_uuid == uuid}
        if not products:
            logger.warning(
                "No discovered product matches uuid=%s; nothing to generate", uuid
            )
            return

    by_store = _group_by_store(products)
    logger.info(
        "Tiler parquet batch for %s: %d store(s) from %d discovered product(s)",
        f"uuid={uuid}" if uuid else "all UUIDs",
        len(by_store),
        len(products),
    )

    batch_config = config.get_tiler_batch_config()

    use_fork = batch_config.use_fork_process
    logger.info("Tiler parquet batch process isolation: use_fork_process=%s", use_fork)

    for store, (store_uuid, variables) in sorted(by_store.items()):
        if use_fork:
            ok = _build_in_subprocess(store, store_uuid, variables, batch_config)
            after_label = f"after child for {store}"
        else:
            ok = build_tiler_parquet(store, store_uuid, variables, batch_config)
            after_label = f"after in-process run for {store}"
        if ok:
            _publish_store(store, products, batch_config.tiler_root_dir)
        else:
            logger.error("Tiler parquet worker failed for store=%s", store)
        log_memory_usage(logger, after_label)


def _publish_store(
    store: str, products: dict[str, ProductIdentity], tiler_root_dir: str
) -> None:
    """Upsert ``store`` into ``root_metadata.json`` right after it converts,
    so it is live without waiting for the rest of the run."""
    # Don't publish a store with no data yet (e.g. every timestamp all-NaN);
    # mapping it to no products drops any old entry.
    if _has_timestamps(tiler_root_dir, store):
        store_products = [p for p in products.values() if p.store == store]
    else:
        logger.warning("Not publishing store with no converted timestamps: %s", store)
        store_products = []
    write_root_metadata({store: store_products}, tiler_root_dir)


def _has_timestamps(tiler_root_dir: str, store: str) -> bool:
    meta = read_metadata(tiler_root_dir, store)
    return meta is not None and bool(meta.timestamps)


def write_root_metadata(
    stores: dict[str, list[ProductIdentity]], tiler_root_dir: str
) -> str:
    """Upsert each store of ``stores`` into ``root_metadata.json``, replacing
    that store's products outright. A store mapped to no products is dropped
    from the file; stores not in this run (other uuids, failed ones) keep their
    entries. Skips the write when nothing changed.
    """
    path = root_metadata_path(tiler_root_dir)

    existing: RootMetadata | None = None
    existing_data = read_json(path, required=False)
    if existing_data is not None:
        candidate = RootMetadata.from_dict(existing_data)
        if candidate.version == ROOT_METADATA_VERSION:
            existing = candidate
        else:
            logger.warning(
                "Root metadata is version %s, not %s; rewriting it from scratch: %s",
                candidate.version,
                ROOT_METADATA_VERSION,
                path,
            )

    merged = dict(existing.stores) if existing is not None else {}
    for store, products in stores.items():
        if products:
            merged[store] = sorted(products, key=lambda p: p.id)
        else:
            merged.pop(store, None)
    if not merged:
        # The tiler refuses an empty catalogue.
        logger.warning("No products to publish; not writing %s", path)
        return path

    meta = RootMetadata(
        version=ROOT_METADATA_VERSION,
        generated_at=datetime.now(timezone.utc).isoformat(),
        stores={store: merged[store] for store in sorted(merged)},
    )
    if existing is not None and existing.stores == meta.stores:
        logger.info("Root metadata unchanged: %s", path)
        return path

    storage.write_json(path, meta.to_dict())
    logger.info(
        "Wrote root metadata: %s (%d store(s), %d product(s) total, "
        "%d store(s) updated this run)",
        path,
        len(meta.stores),
        len(meta.products),
        len(stores),
    )
    return path


def _build_in_subprocess(
    store: str, uuid: str, variables: list[str], batch_config: TilerBatchConfig
) -> bool:
    """Run one store in a forked worker and wait for it."""
    logger.info(
        "Forking tiler parquet worker parent_pid=%s store=%s", os.getpid(), store
    )
    pid = os.fork()
    if pid == 0:
        # Child: always exit here.
        try:
            ok = build_tiler_parquet(store, uuid, variables, batch_config)
            log_memory_usage(logger, f"worker exit ({store})")
            os._exit(0 if ok else 1)
        except BaseException:
            logger.exception("Tiler parquet worker crashed store=%s", store)
            os._exit(1)

    _, status = os.waitpid(pid, 0)
    if os.WIFEXITED(status):
        code = os.WEXITSTATUS(status)
        if code == 0:
            logger.info(
                "Tiler parquet worker finished successfully for store=%s", store
            )
            return True
        logger.error("Tiler parquet worker exit code=%s for store=%s", code, store)
        return False

    termsig = os.WTERMSIG(status) if os.WIFSIGNALED(status) else None
    logger.error(
        "Tiler parquet worker signaled status=%s termsig=%s for store=%s",
        status,
        termsig,
        store,
    )
    return False


def build_tiler_parquet(
    store: str, uuid: str, variables: list[str], batch_config: TilerBatchConfig
) -> bool:
    """Convert one store. Returns False on failure instead of raising."""
    try:
        return _sync_one(store, uuid, variables, batch_config)
    finally:
        close_store(store)


def _sync_one(
    store: str, uuid: str, variables: list[str], batch_config: TilerBatchConfig
) -> bool:
    if open_store(store) is not None:
        return False
    try:
        logger.info("Start syncing tiler parquet for store=%s uuid=%s", store, uuid)
        written, metadata_path = sync_store(
            store,
            uuid,
            variables,
            batch_config.tiler_root_dir,
            max_chunks_per_run=batch_config.max_chunks_per_run,
            duckdb_config=batch_config.duckdb,
        )
        logger.info(
            "Tiler parquet for store=%s synced: %d new timestamp(s), sidecar=%s",
            store,
            len(written),
            metadata_path,
        )
    except Exception as e:
        logger.error(f"Tiler parquet error processing store {store}: {e}")
        return False

    return True
