"""gridded_variables.json normalisation and validation.

The file is the only hand-maintained input to the derived product catalogue, so
every rule that protects a downstream invariant is pinned here rather than left
to be discovered at boot: pair arity (the data-tile encoder packs exactly two
channels), pair order (the shader's R/G assignment), and the scalar-only nature
of visual tiles.
"""

import json

import pytest
from pydantic import ValidationError

from data_access_service.tiler.schemas.gridded_variables import (
    GriddedVariableEntry,
    ProductDefaults,
    load_gridded_variables,
    parse_gridded_variables,
)


def _entry(payload) -> GriddedVariableEntry:
    return GriddedVariableEntry.model_validate(payload)


# --- shorthand normalisation ------------------------------------------------


def test_scalar_shorthand_defaults_to_visual():
    entry = _entry("GSLA")
    assert entry.variable == "GSLA"
    assert entry.variables == ["GSLA"]
    assert entry.visual is True
    assert entry.defaults == ProductDefaults()
    assert entry.overrides == {}


def test_pair_shorthand_defaults_to_data_only():
    entry = _entry(["UCUR", "VCUR"])
    assert entry.variable == ["UCUR", "VCUR"]
    assert entry.visual is False


def test_object_entry_with_defaults_and_overrides():
    entry = _entry(
        {
            "variable": "GSLA",
            "defaults": {"data_tile": {"coastal_fill": {"max_dist_px": 4}}},
            "overrides": {"other.zarr": {"ocean_masked": True}},
        }
    )
    assert entry.defaults.data_tile.coastal_fill.max_dist_px == 4
    assert entry.overrides["other.zarr"].ocean_masked is True


def test_variable_case_is_preserved_verbatim():
    """Names are looked up against the real store schema; only the generated
    product ID lower-cases them."""
    assert _entry("MCS_category").variable == "MCS_category"
    assert _entry("sea_surface_temperature").variable == "sea_surface_temperature"


# --- arity and pair rules ---------------------------------------------------


@pytest.mark.parametrize("variable", [[], ["ONLY"], ["A", "B", "C"]])
def test_list_must_hold_exactly_two_names(variable):
    with pytest.raises(ValidationError):
        _entry({"variable": variable})


def test_single_element_list_is_not_a_scalar():
    """A one-element list would turn a scalar product into a broken vector one,
    so it is rejected rather than silently unwrapped."""
    with pytest.raises(ValidationError):
        _entry(["GSLA"])


def test_pair_order_is_preserved_not_sorted():
    """VCUR before UCUR stays that way — the order is the R/G channel order."""
    assert _entry(["VCUR", "UCUR"]).variable == ["VCUR", "UCUR"]
    assert _entry(["VCUR", "UCUR"]).variables == ["VCUR", "UCUR"]


def test_duplicate_names_in_pair_rejected():
    with pytest.raises(ValidationError):
        _entry(["UCUR", "UCUR"])


@pytest.mark.parametrize("variable", ["", "   ", ["UCUR", ""], ["  ", "VCUR"]])
def test_blank_variable_names_rejected(variable):
    with pytest.raises(ValidationError):
        _entry({"variable": variable})


# --- capability -------------------------------------------------------------


def test_pair_with_visual_true_rejected():
    with pytest.raises(ValidationError):
        _entry({"variable": ["UCUR", "VCUR"], "visual": True})


def test_pair_with_explicit_visual_false_accepted():
    assert _entry({"variable": ["UCUR", "VCUR"], "visual": False}).visual is False


def test_scalar_with_visual_false_accepted():
    """The mechanism that stops a non-renderable scalar (WDIR is the known
    example) advertising visual tiles it cannot serve."""
    assert _entry({"variable": "WDIR", "visual": False}).visual is False


def test_scalar_with_explicit_visual_true_accepted():
    assert _entry({"variable": "GSLA", "visual": True}).visual is True


# --- extra="forbid" at every level -----------------------------------------


def test_unknown_field_on_entry_rejected():
    with pytest.raises(ValidationError):
        _entry({"variable": "GSLA", "vizual": True})


def test_unknown_field_on_defaults_rejected():
    with pytest.raises(ValidationError):
        _entry({"variable": "GSLA", "defaults": {"ocean_maskd": True}})


def test_unknown_field_on_nested_data_tile_rejected():
    with pytest.raises(ValidationError):
        _entry({"variable": "GSLA", "defaults": {"data_tile": {"max_lods": 6}}})


def test_unknown_field_on_override_rejected():
    with pytest.raises(ValidationError):
        _entry({"variable": "GSLA", "overrides": {"x.zarr": {"ocean_maskd": True}}})


def test_unknown_field_on_nested_override_data_tile_rejected():
    with pytest.raises(ValidationError):
        _entry(
            {
                "variable": "GSLA",
                "overrides": {"x.zarr": {"data_tile": {"chunk_pix": [8, 8]}}},
            }
        )


# --- recursive override resolution ------------------------------------------


def test_resolve_defaults_without_override_returns_entry_defaults():
    entry = _entry({"variable": "GSLA", "defaults": {"ocean_masked": True}})
    assert entry.resolve_defaults("anything.zarr").ocean_masked is True


def test_override_applies_only_to_its_own_dataset():
    entry = _entry(
        {
            "variable": ["UCUR", "VCUR"],
            "overrides": {
                "model_sea_level_anomaly_gridded_realtime.zarr": {"ocean_masked": True}
            },
        }
    )
    assert (
        entry.resolve_defaults(
            "model_sea_level_anomaly_gridded_realtime.zarr"
        ).ocean_masked
        is True
    )
    # The 18 HF-radar grids matched by the same specification must not inherit
    # the SLA-grid ocean mask.
    assert (
        entry.resolve_defaults(
            "radar_TurquoiseCoast_velocity_hourly_delayed_qc.zarr"
        ).ocean_masked
        is False
    )


def test_partial_nested_override_inherits_sibling_defaults():
    """An omitted override field inherits the default; it does not reset the
    whole nested block back to the model defaults."""
    entry = _entry(
        {
            "variable": "GSLA",
            "defaults": {
                "data_tile": {
                    "padding": 3,
                    "coastal_fill": {"max_dist_px": 4},
                }
            },
            "overrides": {"x.zarr": {"data_tile": {"padding": 9}}},
        }
    )
    resolved = entry.resolve_defaults("x.zarr")
    assert resolved.data_tile.padding == 9
    assert resolved.data_tile.coastal_fill.max_dist_px == 4
    # Untouched entry defaults are not mutated by resolution.
    assert entry.defaults.data_tile.padding == 3


def test_override_leaves_unmentioned_top_level_fields_alone():
    entry = _entry(
        {
            "variable": "GSLA",
            "defaults": {
                "ocean_masked": True,
                "visual_tile": {"coastal_fill": {"max_dist_px": 8}},
            },
            "overrides": {"x.zarr": {"data_tile": {"padding": 9}}},
        }
    )
    resolved = entry.resolve_defaults("x.zarr")
    assert resolved.ocean_masked is True
    assert resolved.visual_tile.coastal_fill.max_dist_px == 8


# --- file loading -----------------------------------------------------------


def test_parse_rejects_non_array():
    with pytest.raises(ValueError):
        parse_gridded_variables({"variable": "GSLA"})


def test_parse_rejects_empty_array():
    with pytest.raises(ValueError):
        parse_gridded_variables([])


def test_parse_mixed_shorthand_and_object_entries():
    entries = parse_gridded_variables(
        ["GSLA", ["UCUR", "VCUR"], {"variable": "GSL", "visual": False}]
    )
    assert [e.variable for e in entries] == ["GSLA", ["UCUR", "VCUR"], "GSL"]
    assert [e.visual for e in entries] == [True, False, False]


def test_load_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_gridded_variables(tmp_path / "absent.json")


def test_load_malformed_json_raises(tmp_path):
    path = tmp_path / "gridded_variables.json"
    path.write_text("not json at all")
    with pytest.raises(json.JSONDecodeError):
        load_gridded_variables(path)


def test_load_invalid_entry_raises(tmp_path):
    path = tmp_path / "gridded_variables.json"
    path.write_text(json.dumps([["A", "B", "C"]]))
    with pytest.raises(ValidationError):
        load_gridded_variables(path)


def test_load_valid_file(tmp_path):
    path = tmp_path / "gridded_variables.json"
    path.write_text(json.dumps(["GSLA", ["UCUR", "VCUR"]]))
    entries = load_gridded_variables(path)
    assert [e.variables for e in entries] == [["GSLA"], ["UCUR", "VCUR"]]
