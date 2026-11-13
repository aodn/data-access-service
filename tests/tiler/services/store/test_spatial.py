from types import SimpleNamespace

import numpy as np
import pytest

from data_access_service.tiler.services.store import spatial


def _store(monkeypatch, lon: np.ndarray) -> None:
    meta = SimpleNamespace(lat=np.array([-60.0, 10.0]), lon=lon)
    monkeypatch.setattr(spatial, "get_store_metadata", lambda store: meta)


@pytest.mark.parametrize(
    "lon, expected",
    [
        # Within -180..180: unchanged.
        (np.arange(96.0, 174.01, 0.5), (96.0, 174.0)),
        # GSLA: 57..185 crosses 180, so west > east.
        (np.arange(57.0, 185.01, 0.2), (57.0, -175.0)),
        # Ends at exactly 180: stays 180, not -180.
        (np.arange(170.0, 180.01, 0.5), (170.0, 180.0)),
        # Wholly past 180.
        (np.arange(190.0, 250.01, 1.0), (-170.0, -110.0)),
        # Global 0..360 grid: the whole world.
        (np.arange(0.0, 360.0, 2.0), (-180.0, 180.0)),
        # Global -180..180 grid: the whole world too.
        (np.arange(-179.875, 180.0, 0.25), (-180.0, 180.0)),
    ],
)
def test_default_bbox_is_in_minus_180_to_180(monkeypatch, lon, expected):
    _store(monkeypatch, lon)
    w, s, e, n = spatial.default_bbox_from_store("any")
    assert (w, e) == (pytest.approx(expected[0]), pytest.approx(expected[1]))
    assert (s, n) == (-60.0, 10.0)


def test_native_resolution_counts_cells_across_180(monkeypatch):
    _store(monkeypatch, np.arange(57.0, 185.01, 0.2))
    w, _ = spatial.native_resolution_in_bbox("any", (170.0, -10.0, -175.0, 10.0))
    assert w == 75  # 15 degrees at 0.2
