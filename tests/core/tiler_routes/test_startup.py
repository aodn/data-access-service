"""Tiler warmup sequencing, readiness, and the legacy-product startup gate.

The shape being defended: nothing is published before it is verified, and every
fatal path leaves the tiler unready rather than serving a catalogue that is
quietly wrong. A tiler that boots ready with a live product silently missing is
the failure this whole design exists to make loud.
"""

import asyncio
import re

import pytest

from data_access_service.core.tiler_routes import shared, startup
from data_access_service.core.tiler_routes.startup import (
    assert_legacy_products_intact,
    run_tiler_warmup,
)
from data_access_service.tiler.services.product.product import (
    CoastalFill,
    DataTileConfig,
    Product,
)
from data_access_service.tiler.services.product.verification import (
    LEGACY_PRODUCT_IDS,
    VerificationResult,
)

GSLA_ID = "model_sea_level_anomaly_gridded_realtime:gsla"
CURRENTS_ID = "model_sea_level_anomaly_gridded_realtime:ucur+vcur"


def _legacy_catalogue() -> dict[str, Product]:
    """All five legacy products, intact, as a healthy boot would produce them."""
    products = {
        pid: Product(
            id=pid,
            source_path=f"s3://b/{pid.split(':')[0]}.zarr",
            variable=pid.split(":")[1],
        )
        for pid in LEGACY_PRODUCT_IDS
    }
    products[GSLA_ID] = Product(
        id=GSLA_ID,
        source_path="s3://b/model_sea_level_anomaly_gridded_realtime.zarr",
        variable="GSLA",
        data_tile=DataTileConfig(coastal_fill=CoastalFill(max_dist_px=4)),
    )
    products[CURRENTS_ID] = Product(
        id=CURRENTS_ID,
        source_path="s3://b/model_sea_level_anomaly_gridded_realtime.zarr",
        variable=["UCUR", "VCUR"],
        ocean_masked=True,
        visual=False,
    )
    return products


# --- assert_legacy_products_intact ------------------------------------------


def test_passes_on_a_catalogue_with_all_five_intact():
    assert_legacy_products_intact(_legacy_catalogue())


def test_extra_derived_products_do_not_disturb_the_gate():
    catalogue = _legacy_catalogue()
    catalogue["radar_site:ucur+vcur"] = Product(
        id="radar_site:ucur+vcur",
        source_path="s3://b/radar_site.zarr",
        variable=["UCUR", "VCUR"],
    )
    assert_legacy_products_intact(catalogue)


@pytest.mark.parametrize("missing_id", sorted(LEGACY_PRODUCT_IDS))
def test_raises_when_any_legacy_id_is_missing(missing_id):
    """A variable can still match other datasets while its original dataset is
    dropped, leaving a large, healthy-looking catalogue with a live product
    silently gone from the portal."""
    catalogue = _legacy_catalogue()
    del catalogue[missing_id]

    # re.escape: the vector product's id contains a literal '+'.
    with pytest.raises(RuntimeError, match=re.escape(missing_id)):
        assert_legacy_products_intact(catalogue)


def test_raises_when_gsla_lost_its_coastal_fill():
    catalogue = _legacy_catalogue()
    catalogue[GSLA_ID] = Product(
        id=GSLA_ID,
        source_path="s3://b/model_sea_level_anomaly_gridded_realtime.zarr",
        variable="GSLA",
    )

    with pytest.raises(RuntimeError, match="coastal_fill"):
        assert_legacy_products_intact(catalogue)


def test_raises_when_gsla_coastal_fill_distance_changed():
    catalogue = _legacy_catalogue()
    catalogue[GSLA_ID] = Product(
        id=GSLA_ID,
        source_path="s3://b/model_sea_level_anomaly_gridded_realtime.zarr",
        variable="GSLA",
        data_tile=DataTileConfig(coastal_fill=CoastalFill(max_dist_px=9)),
    )

    with pytest.raises(RuntimeError, match="coastal_fill"):
        assert_legacy_products_intact(catalogue)


def test_raises_when_currents_lost_ocean_masked():
    """The dataset override is the only thing applying the mask; if its key
    stops matching, the product renders wrong rather than failing."""
    catalogue = _legacy_catalogue()
    catalogue[CURRENTS_ID] = Product(
        id=CURRENTS_ID,
        source_path="s3://b/model_sea_level_anomaly_gridded_realtime.zarr",
        variable=["UCUR", "VCUR"],
        ocean_masked=False,
    )

    with pytest.raises(RuntimeError, match="ocean_masked"):
        assert_legacy_products_intact(catalogue)


# --- warmup sequencing ------------------------------------------------------


class FakeAPI:
    def __init__(self, ready=True, index=None):
        self._ready = ready
        self._index = index if index is not None else {"u1": {}}
        self.wait_timeouts: list[float | None] = []

    async def wait_until_ready(self, timeout=300):
        self.wait_timeouts.append(timeout)
        return self._ready

    def get_dataset_variables(self, uuid=None):
        return self._index


@pytest.fixture
def warmup_env(monkeypatch):
    """Stub every step around discovery so ordering can be observed directly."""
    calls: list[str] = []
    state = {
        "candidates": {"a:v": Product(id="a:v", source_path="s3://b/a.zarr", variable="v")},
        "outcomes": {"s3://b/a.zarr": None},
        "result": None,
        "published": None,
        "ready": False,
    }
    state["result"] = VerificationResult(products=dict(state["candidates"]))

    def record(name, value=None):
        def _fn(*args, **kwargs):
            calls.append(name)
            return value

        return _fn

    async def fake_prewarm(urls):
        calls.append("prewarm")
        state["prewarm_urls"] = urls
        return state["outcomes"]

    def fake_verify(candidates, outcomes):
        calls.append("verify")
        return state["result"]

    def fake_publish(products):
        calls.append("publish")
        state["published"] = products

    def fake_mark_ready():
        calls.append("mark_ready")
        state["ready"] = True

    monkeypatch.setattr(startup, "load_gridded_variables", record("load_config", []))
    monkeypatch.setattr(
        startup,
        "build_candidate_products",
        lambda *a, **k: (calls.append("discover"), state["candidates"])[1],
    )
    monkeypatch.setattr(startup, "reject_unmatched_overrides", record("overrides"))
    monkeypatch.setattr(startup, "load_colormaps", record("colormaps"))
    monkeypatch.setattr(startup, "warmup_resample", record("resample"))
    monkeypatch.setattr(startup, "warmup_visual", record("visual"))
    monkeypatch.setattr(startup, "prewarm_stores", fake_prewarm)
    monkeypatch.setattr(startup, "verify_candidate_products", fake_verify)
    monkeypatch.setattr(startup, "publish_products", fake_publish)
    monkeypatch.setattr(startup, "mark_tiler_ready", fake_mark_ready)
    monkeypatch.setattr(startup, "assert_legacy_products_intact", record("legacy_gate"))

    return calls, state


@pytest.mark.asyncio
async def test_happy_path_publishes_then_marks_ready(warmup_env):
    calls, state = warmup_env
    await run_tiler_warmup(FakeAPI())

    assert state["ready"] is True
    assert state["published"] == state["result"].products
    # Verification precedes publication precedes readiness.
    assert calls.index("prewarm") < calls.index("verify")
    assert calls.index("verify") < calls.index("publish")
    assert calls.index("legacy_gate") < calls.index("publish")
    assert calls.index("publish") < calls.index("mark_ready")


@pytest.mark.asyncio
async def test_warmup_waits_indefinitely_for_metadata(warmup_env):
    api = FakeAPI()
    await run_tiler_warmup(api)
    assert api.wait_timeouts == [None]


@pytest.mark.asyncio
async def test_unready_api_leaves_the_tiler_unready(warmup_env, caplog):
    calls, state = warmup_env

    with caplog.at_level("CRITICAL"):
        await run_tiler_warmup(FakeAPI(ready=False))

    assert state["ready"] is False
    assert "discover" not in calls
    assert any(r.levelname == "CRITICAL" for r in caplog.records)


@pytest.mark.asyncio
async def test_prewarm_receives_every_unique_candidate_source_path(warmup_env):
    calls, state = warmup_env
    state["candidates"] = {
        "a:v": Product(id="a:v", source_path="s3://b/a.zarr", variable="v"),
        "a:w": Product(id="a:w", source_path="s3://b/a.zarr", variable="w"),
        "b:v": Product(id="b:v", source_path="s3://b/b.zarr", variable="v"),
    }
    state["result"] = VerificationResult(products=dict(state["candidates"]))
    state["outcomes"] = {"s3://b/a.zarr": None, "s3://b/b.zarr": None}

    await run_tiler_warmup(FakeAPI())

    # Deduplicated and sorted — 85 products but only 60 opens.
    assert state["prewarm_urls"] == ["s3://b/a.zarr", "s3://b/b.zarr"]


@pytest.mark.asyncio
async def test_verification_dropping_everything_leaves_the_tiler_unready(
    warmup_env, caplog
):
    calls, state = warmup_env
    state["result"] = VerificationResult(products={})

    with caplog.at_level("CRITICAL"):
        await run_tiler_warmup(FakeAPI())

    assert state["ready"] is False
    assert state["published"] is None
    assert "publish" not in calls


@pytest.mark.asyncio
async def test_legacy_gate_failure_prevents_publication(warmup_env, monkeypatch, caplog):
    calls, state = warmup_env

    def boom(products):
        raise RuntimeError("legacy product missing")

    monkeypatch.setattr(startup, "assert_legacy_products_intact", boom)

    with caplog.at_level("CRITICAL"):
        await run_tiler_warmup(FakeAPI())

    assert state["ready"] is False
    assert "publish" not in calls
    assert any("Tiler warmup failed" in r.message for r in caplog.records)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failing_step",
    [
        "load_gridded_variables",
        "build_candidate_products",
        "reject_unmatched_overrides",
        "publish_products",
    ],
)
async def test_any_fatal_step_leaves_readiness_false(
    warmup_env, failing_step, monkeypatch, caplog
):
    calls, state = warmup_env

    def boom(*args, **kwargs):
        raise RuntimeError(f"{failing_step} exploded")

    monkeypatch.setattr(startup, failing_step, boom)

    with caplog.at_level("CRITICAL"):
        await run_tiler_warmup(FakeAPI())

    assert state["ready"] is False
    assert any(r.levelname == "CRITICAL" for r in caplog.records)


@pytest.mark.asyncio
async def test_cancellation_is_re_raised_not_logged_as_failure(
    warmup_env, monkeypatch, caplog
):
    """Warmup runs as a lifespan task whose result is never awaited. Swallowing
    CancelledError would turn every shutdown into a spurious CRITICAL."""
    calls, state = warmup_env

    async def cancelled(urls):
        raise asyncio.CancelledError()

    monkeypatch.setattr(startup, "prewarm_stores", cancelled)

    with caplog.at_level("CRITICAL"):
        with pytest.raises(asyncio.CancelledError):
            await run_tiler_warmup(FakeAPI())

    assert not any("Tiler warmup failed" in r.message for r in caplog.records)


@pytest.fixture(autouse=True)
def restore_tiler_readiness():
    """run_tiler_warmup flips module-level readiness; put it back afterwards."""
    saved = shared._tiler_ready
    yield
    shared._tiler_ready = saved
