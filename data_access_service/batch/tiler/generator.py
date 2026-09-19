"""The tiler batch job: convert every product's zarr store to parquet and
publish ``root_metadata.json``.

Stores run one at a time, each in a forked worker (or in-process when
``use_fork_process`` is off), so only one is in memory. Runs are
incremental and safe to repeat.
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
    """Convert every product's store, then update ``root_metadata.json``.
    ``uuid`` limits the run to one metadata record."""
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

    succeeded: set[str] = set()
    for store, (store_uuid, variables) in sorted(by_store.items()):
        if use_fork:
            ok = _build_in_subprocess(store, store_uuid, variables, batch_config)
            after_label = f"after child for {store}"
        else:
            ok = build_tiler_parquet(store, store_uuid, variables, batch_config)
            after_label = f"after in-process run for {store}"
        if ok:
            succeeded.add(store)
        else:
            logger.error("Tiler parquet worker failed for store=%s", store)
        log_memory_usage(logger, after_label)

    # Don't publish stores with no data yet (e.g. every timestamp all-NaN).
    empty = {
        store
        for store in succeeded
        if not _has_timestamps(batch_config.output_dir, store)
    }
    if empty:
        logger.warning(
            "Not publishing %d store(s) with no converted timestamps: %s",
            len(empty),
            sorted(empty),
        )
    published = [
        p for p in products.values() if p.store in succeeded and p.store not in empty
    ]
    unpublished = [p.id for p in products.values() if p.store in empty]
    write_root_metadata(published, batch_config.output_dir, remove=unpublished)


def _has_timestamps(output_dir: str, store: str) -> bool:
    meta = read_metadata(output_dir, store)
    return meta is not None and bool(meta.timestamps)


def write_root_metadata(
    products: list[ProductIdentity], output_dir: str, remove: list[str] = ()
) -> str:
    """Upsert ``products`` into ``root_metadata.json`` and drop the ids in
    ``remove``. Products not in this run (other uuids, failed stores) keep
    their entries. Skips the write when nothing changed.
    """
    path = root_metadata_path(output_dir)

    existing: RootMetadata | None = None
    existing_by_id: dict[str, dict] = {}
    existing_data = storage.read_json(path)
    if existing_data is not None:
        existing = RootMetadata.from_dict(existing_data)
        existing_by_id = {p["id"]: p for p in existing.products}

    merged = dict(existing_by_id)
    for product in products:
        merged[product.id] = product.to_dict()
    for pid in remove:
        merged.pop(pid, None)
    if not merged:
        # The tiler refuses an empty catalogue.
        logger.warning("No products to publish; not writing %s", path)
        return path

    meta = RootMetadata(
        version=ROOT_METADATA_VERSION,
        generated_at=datetime.now(timezone.utc).isoformat(),
        products=[merged[pid] for pid in sorted(merged)],
    )
    if (
        existing is not None
        and existing.version == meta.version
        and existing.products == meta.products
    ):
        logger.info("Root metadata unchanged: %s", path)
        return path

    storage.write_json(path, meta.to_dict())
    logger.info(
        "Wrote root metadata: %s (%d product(s) total, %d updated this run)",
        path,
        len(meta.products),
        len(products),
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
            batch_config.output_dir,
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
