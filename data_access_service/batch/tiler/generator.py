"""Run the zarr -> parquet conversion for one or every gridded tiler product.

This is now the only place that discovers products from live metadata
(``discovery.discover_products``) or opens real zarr stores
(``zarr_registry.prewarm_stores``) - the live tiler API reads what this job
publishes instead (``root_metadata.json`` + each store's ``metadata.json``
sidecar), never live metadata or zarr directly.

Mirrors ``batch.estimation.generator``: one store per fork so DuckDB/xarray
memory goes back to the OS between stores.

Writes to a local directory (``TilerParquetConfig.output_dir``) - S3 upload is
follow-up work, not implemented yet.
"""

import json
import os
import threading
from datetime import datetime, timezone

import anyio

from data_access_service import Config, init_log
from data_access_service.batch.tiler.discovery import discover_products
from data_access_service.batch.tiler.parquet_generator import generate_parquet
from data_access_service.batch.tiler.zarr_registry import prewarm_stores
from data_access_service.core.api import API
from data_access_service.models.tiler_parquet_types import (
    ROOT_METADATA_VERSION,
    RootMetadata,
)
from data_access_service.models.tiler_types import TilerParquetConfig
from data_access_service.tiler.services.product.product import Product
from data_access_service.utils.memory_utils import log_memory_usage

config = Config.get_config()
logger = init_log(config)

# Same reason as estimation/pmtiles: one run per process, because the build
# owns the process-global store registry and DuckDB connections.
_generation_lock = threading.Lock()


class TilerParquetGenerationInProgressError(RuntimeError):
    """Raised when a build is requested while another is already running."""


def _group_by_store(products: dict[str, Product]) -> dict[str, tuple[str, list[str]]]:
    """``source_path -> (uuid, sorted variable names)``, merged across every
    product discovered for that store - discovery fans one store out into
    several ``Product``s, one per ``gridded_variables`` spec, but the
    conversion only needs to touch each store once.
    """
    grouped: dict[str, tuple[str, set[str]]] = {}
    for product in products.values():
        store_uuid, variables = grouped.get(
            product.source_path, (product.metadata_uuid, set())
        )
        variables.update(product.variables)
        grouped[product.source_path] = (store_uuid, variables)
    return {url: (uuid, sorted(vars_)) for url, (uuid, vars_) in grouped.items()}


def generate_tiler_parquet_for_all_products(api: API, uuid: str | None = None) -> None:
    """Convert every discovered gridded product's backing store to parquet.

    Args:
        api: Initialized API with metadata loaded.
        uuid: Optional metadata UUID filter (local/debug or a Batch parameter).
    """
    base_url = config.get_tiler_config().co_bucket
    products = discover_products(api, base_url)

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

    outcomes = anyio.run(prewarm_stores, sorted(by_store))
    failed = {url for url, outcome in outcomes.items() if outcome is not None}
    if failed:
        logger.warning(
            "Skipping %d store(s) that failed prewarm: %s", len(failed), sorted(failed)
        )
    work = {url: v for url, v in by_store.items() if url not in failed}
    if not work:
        logger.warning("No store passed prewarm; nothing to generate")

    tp_config = config.get_tiler_parquet_config()
    logger.info(
        "Tiler parquet batch process isolation: use_fork_process=%s",
        tp_config.use_fork_process,
    )

    succeeded: set[str] = set()
    for store_url, (store_uuid, variables) in sorted(work.items()):
        if tp_config.use_fork_process:
            ok = _build_in_subprocess(store_url, store_uuid, variables, tp_config)
            after_label = f"after child for {store_url}"
        else:
            ok = build_tiler_parquet(store_url, store_uuid, variables, tp_config)
            after_label = f"after in-process run for {store_url}"
        if ok:
            succeeded.add(store_url)
        else:
            logger.error("Tiler parquet worker failed for store=%s", store_url)
        log_memory_usage(logger, after_label)

    published = [p for p in products.values() if p.source_path in succeeded]
    write_root_metadata(published, tp_config.output_dir)


def write_root_metadata(products: list[Product], output_dir: str) -> str:
    """Upsert ``products`` (by id) into ``root_metadata.json``, so the tiler
    API can rebuild its whole product catalogue from this one file - no live
    metadata call, no zarr open.

    Upserts rather than overwrites: a ``uuid``-scoped run only touches that
    uuid's products, so it must not wipe out every other uuid's entries. A
    full (no-``uuid``) run's ``products`` is the complete current catalogue,
    so it ends up superseding every entry anyway.
    """
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "root_metadata.json")

    existing_by_id: dict[str, dict] = {}
    if os.path.exists(path):
        with open(path) as f:
            existing = RootMetadata.from_dict(json.load(f))
        existing_by_id = {p["id"]: p for p in existing.products}

    for product in products:
        existing_by_id[product.id] = product.to_dict()

    meta = RootMetadata(
        version=ROOT_METADATA_VERSION,
        generated_at=datetime.now(timezone.utc).isoformat(),
        products=[existing_by_id[pid] for pid in sorted(existing_by_id)],
    )
    with open(path, "w") as f:
        json.dump(meta.to_dict(), f)
    logger.info(
        "Wrote root metadata: %s (%d product(s) total, %d updated this run)",
        path,
        len(meta.products),
        len(products),
    )
    return path


def _build_in_subprocess(
    store_url: str, uuid: str, variables: list[str], tp_config: TilerParquetConfig
) -> bool:
    """Fork a worker for one store; wait until it exits."""
    logger.info(
        "Forking tiler parquet worker parent_pid=%s store=%s", os.getpid(), store_url
    )
    pid = os.fork()
    if pid == 0:
        # Child: never return into the parent loop.
        try:
            ok = build_tiler_parquet(store_url, uuid, variables, tp_config)
            log_memory_usage(logger, f"worker exit ({store_url})")
            os._exit(0 if ok else 1)
        except BaseException:
            logger.exception("Tiler parquet worker crashed store=%s", store_url)
            os._exit(1)

    _, status = os.waitpid(pid, 0)
    if os.WIFEXITED(status):
        code = os.WEXITSTATUS(status)
        if code == 0:
            logger.info(
                "Tiler parquet worker finished successfully for store=%s", store_url
            )
            return True
        logger.error("Tiler parquet worker exit code=%s for store=%s", code, store_url)
        return False

    termsig = os.WTERMSIG(status) if os.WIFSIGNALED(status) else None
    logger.error(
        "Tiler parquet worker signaled status=%s termsig=%s for store=%s",
        status,
        termsig,
        store_url,
    )
    return False


def generate_tiler_parquet_for_store(
    store_url: str, uuid: str, variables: list[str]
) -> bool:
    """One store, rejecting the call when another build is already running."""
    if not _generation_lock.acquire(blocking=False):
        raise TilerParquetGenerationInProgressError(
            "Another tiler parquet generation is already running in this "
            f"process; rejected request for store {store_url}. Retry later."
        )
    try:
        return build_tiler_parquet(
            store_url, uuid, variables, config.get_tiler_parquet_config()
        )
    finally:
        _generation_lock.release()


def build_tiler_parquet(
    store_url: str, uuid: str, variables: list[str], tp_config: TilerParquetConfig
) -> bool:
    """Build one store's parquet + sidecar into ``tp_config.output_dir``.

    Never raises: a failed store must not fail the whole batch.
    """
    try:
        logger.info(
            "Start generating tiler parquet for store=%s uuid=%s", store_url, uuid
        )
        value_paths, metadata_path = generate_parquet(
            store_url,
            uuid,
            variables,
            tp_config.output_dir,
            batch_days=tp_config.batch_days,
            max_timestamps=tp_config.max_timestamps,
        )
        logger.info(
            "Tiler parquet for store=%s written: sidecar=%s values=%s",
            store_url,
            metadata_path,
            list(value_paths.values()),
        )
    except Exception as e:
        logger.error(f"Tiler parquet error processing store {store_url}: {e}")
        return False

    return True
