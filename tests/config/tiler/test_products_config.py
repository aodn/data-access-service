"""The committed products.json is valid and still says what it means.

Same rationale as test_gridded_variables_config.py: load the real committed
file through the real validator so a malformed or semantically-drifted seed
fails in CI rather than leaving the tiler at 503 on deploy.
"""

from data_access_service.tiler.schemas.products import load_product_overrides

SLA_GSLA = "model_sea_level_anomaly_gridded_realtime:gsla"
SLA_CURRENTS = "model_sea_level_anomaly_gridded_realtime:ucur+vcur"


def test_committed_config_loads_and_validates():
    overrides = load_product_overrides()
    assert overrides, "the committed seed must not be empty"


def test_gsla_keeps_its_coastal_fill():
    """Live behaviour today: without it the GSLA grid renders with a visible
    gap along the coast."""
    override = load_product_overrides()[SLA_GSLA]
    assert override.data_tile.coastal_fill is not None
    assert override.data_tile.coastal_fill.max_dist_px == 4


def test_currents_pair_masks_only_the_sla_grid():
    """The committed ocean mask is built from the SLA grid. The 18 HF-radar
    sites the UCUR/VCUR specification also matches sit on entirely different
    grids and must not inherit it — so the override is keyed to this one
    product id, not to the variable pair generally."""
    overrides = load_product_overrides()
    assert overrides[SLA_CURRENTS].ocean_masked is True
    assert "radar_TurquoiseCoast_velocity_hourly_delayed_qc:ucur+vcur" not in overrides


def test_no_duplicate_override_ids():
    overrides = load_product_overrides()
    assert len(overrides) == len({SLA_GSLA, SLA_CURRENTS})
