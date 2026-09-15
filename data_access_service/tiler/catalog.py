"""Load visual products from precomputed parquet sidecar meta files."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from data_access_service.config.config import Config
from data_access_service.core.duckdbclient import TilerDuckDBClient
from data_access_service.tiler.product import Product, load_products
from data_access_service.tiler.utils.dates import iso_timestamp

logger = logging.getLogger(__name__)

_CLIENT: TilerDuckDBClient | None = None


def get_client() -> TilerDuckDBClient:
    global _CLIENT
    if _CLIENT is None:
        cfg = Config.get_config().get_tiler_vector_config()
        _CLIENT = TilerDuckDBClient(cfg)
    return _CLIENT


def close_client() -> None:
    global _CLIENT
    if _CLIENT is not None:
        _CLIENT.close()
        _CLIENT = None


def _product_id(dataset: str, variable: str) -> str:
    stem = dataset.removesuffix(".zarr")
    return f"{stem}:{variable.lower()}"


def _products_from_meta(payload: dict, parquet_uri: str) -> list[Product]:
    uuid = payload.get("uuid") or ""
    timestamps = tuple(payload.get("timestamps") or [])
    out: list[Product] = []
    for frag in payload.get("variables") or []:
        variable = frag.get("variable") or "value"
        dataset = frag.get("dataset") or uuid
        out.append(
            Product(
                id=_product_id(dataset, variable),
                uuid=uuid,
                dataset=dataset,
                variable=variable,
                timestamps=timestamps or tuple(frag.get("timestamps") or []),
                n_i=int(frag.get("n_i") or 0),
                n_j=int(frag.get("n_j") or 0),
                lat_min=float(frag.get("lat_min") or 0.0),
                lat_max=float(frag.get("lat_max") or 0.0),
                lon_min=float(frag.get("lon_min") or 0.0),
                lon_max=float(frag.get("lon_max") or 0.0),
                vmin=float(frag.get("vmin") or 0.0),
                vmax=float(frag.get("vmax") or 1.0),
                source_path=payload.get("parquet") or parquet_uri,
            )
        )
    return out


def _load_local_metas(output_dir: str) -> list[Product]:
    products: list[Product] = []
    root = Path(output_dir)
    if not root.is_dir():
        return products
    for path in sorted(root.glob("*.meta.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.exception("Failed to read %s", path)
            continue
        parquet = str(path).replace(".meta.json", ".parquet")
        products.extend(_products_from_meta(payload, parquet))
    return products


def _load_s3_metas(bucket: str, prefix: str) -> list[Product]:
    products: list[Product] = []
    s3 = Config.get_config().get_s3_client()
    if s3 is None:
        logger.warning("No S3 client; skip listing s3://%s/%s", bucket, prefix)
        return products
    prefix = prefix.strip("/") + "/"
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents") or []:
            key = obj["Key"]
            if not key.endswith(".meta.json"):
                continue
            try:
                body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
                payload = json.loads(body)
            except Exception:
                logger.exception("Failed to read s3://%s/%s", bucket, key)
                continue
            parquet_key = key[: -len(".meta.json")] + ".parquet"
            products.extend(
                _products_from_meta(payload, f"s3://{bucket}/{parquet_key}")
            )
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return products


def refresh_catalog() -> dict[str, Product]:
    """Scan local and/or S3 metas and publish the product registry."""
    cfg = Config.get_config().get_tiler_vector_config()
    found: list[Product] = []
    found.extend(_load_local_metas(cfg.output_dir))
    if cfg.write_s3:
        try:
            found.extend(_load_s3_metas(cfg.s3_bucket, cfg.s3_prefix))
        except Exception:
            logger.exception("Listing S3 vector metas failed")
    by_id: dict[str, Product] = {}
    for product in found:
        by_id[product.id] = product
    load_products(by_id)
    logger.info("Loaded %s parquet-backed tiler product(s)", len(by_id))
    return by_id


def available_dates_iso(product: Product) -> list[str]:
    return [iso_timestamp(ts) for ts in product.timestamps]
