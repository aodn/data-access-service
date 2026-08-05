"""The committed gridded_variables.json is valid and still says what it means.

Discovery only ever sees this file at boot, against real metadata. Loading it
through the same validator here makes a malformed or semantically-drifted seed
fail in CI rather than leaving the tiler at 503 on deploy.
"""

from data_access_service.tiler.schemas.gridded_variables import (
    load_gridded_variables,
)

SLA_DATASET = "model_sea_level_anomaly_gridded_realtime.zarr"


def _by_variable(entries):
    return {str(entry.variable): entry for entry in entries}


def test_committed_config_loads_and_validates():
    entries = load_gridded_variables()
    assert entries, "the committed seed must not be empty"


def test_seed_has_the_expected_specification_shape():
    """19 specifications: 18 scalars plus one ordered pair. The pair is the one
    that fans out across 19 differently-gridded stores, so its arity is what the
    per-dataset override in this file exists to work around."""
    entries = load_gridded_variables()
    pairs = [e for e in entries if isinstance(e.variable, list)]
    scalars = [e for e in entries if isinstance(e.variable, str)]

    assert len(entries) == 19
    assert len(scalars) == 18
    assert [e.variable for e in pairs] == [["UCUR", "VCUR"]]


def test_no_duplicate_specifications():
    entries = load_gridded_variables()
    keys = [str(e.variable) for e in entries]
    assert len(keys) == len(set(keys))


def test_gsla_keeps_its_coastal_fill():
    """Live behaviour today: without it the GSLA grid renders with a visible
    gap along the coast."""
    entry = _by_variable(load_gridded_variables())["GSLA"]
    assert entry.defaults.data_tile.coastal_fill is not None
    assert entry.defaults.data_tile.coastal_fill.max_dist_px == 4


def test_currents_pair_masks_only_the_sla_grid():
    """The committed ocean mask is built from the SLA grid. The 18 HF-radar
    sites this specification also matches sit on entirely different grids and
    must not inherit it."""
    entry = _by_variable(load_gridded_variables())["['UCUR', 'VCUR']"]

    assert entry.visual is False
    assert set(entry.overrides) == {SLA_DATASET}
    assert entry.resolve_defaults(SLA_DATASET).ocean_masked is True
    assert entry.resolve_defaults("radar_site.zarr").ocean_masked is False


def test_no_scalar_in_the_seed_disables_visual_tiles():
    """Documents the phase-1 state rather than constraining it: the field is
    implemented and on the wire, but no curated scalar needs it yet. Update this
    when the first one (WDIR is the known candidate) is added."""
    entries = load_gridded_variables()
    scalars = [e for e in entries if isinstance(e.variable, str)]
    assert all(e.visual for e in scalars)
