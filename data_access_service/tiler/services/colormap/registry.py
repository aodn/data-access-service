import json
import logging
from pathlib import Path
from typing import Literal

from data_access_service.config.tiler.paths import COLORMAPS_CONFIG_PATH
from data_access_service.tiler.utils.colors import categorical_lut, parse_color

logger = logging.getLogger(__name__)

ColormapMode = Literal["ramp", "categorical"]

_config_path = Path(COLORMAPS_CONFIG_PATH)
_custom_colormaps: dict[str, list[tuple[int, int, int, int]]] = {}
_custom_colormap_modes: dict[str, ColormapMode] = {}
# Category values (sorted) for categorical colormaps. The 256-LUT alone can't
# recover them — transparent categories look identical to unmapped slots — so we
# store them explicitly to validate a colormap against a product's flag_values at
# request time (see [[colormap.categorical]] callers).
_custom_colormap_values: dict[str, list[int]] = {}


def get_colormap(name: str) -> list[tuple[int, int, int, int]] | None:
    """Return the 256-entry LUT for a custom colormap, or None if not registered."""
    return _custom_colormaps.get(name)


def is_categorical(name: str) -> bool:
    """Return True if the colormap was registered in categorical mode."""
    return _custom_colormap_modes.get(name) == "categorical"


def get_category_values(name: str) -> list[int] | None:
    """Return the sorted category values of a categorical colormap, or None.

    None means the name is unknown or was not registered as categorical.
    """
    return _custom_colormap_values.get(name)


def load_colormaps() -> None:
    """Read colormaps.json from disk into the in-memory registry. Called once on startup.

    ``"ramp"`` entries store an ``entries`` array: a dense 256-slot RGBA gradient,
    since rio-tiler's ``apply_cmap`` only takes its fast ``make_lut`` path over a
    full 256-entry table and a continuous ramp genuinely needs every slot.

    ``"categorical"`` entries instead store ``values`` and ``colors`` as parallel
    arrays — one colour per real category code, self-explanatory on disk. At load
    time each is expanded into the same 256-entry shape via ``categorical_lut``
    (see [[utils.colors]]), which places each colour at the slot equal to its own
    value (no rescaling), so distinct codes can never collide onto the same slot.
    ``values`` is also kept alongside, unexpanded, since the 256-LUT alone can't
    distinguish a transparent category from an unmapped slot.

    A malformed file (bad JSON, or a colormap failing the checks in ``_reload``)
    is logged as an error and otherwise swallowed — the tiler starts up with no
    custom colormaps rather than refusing to start, since rio-tiler/matplotlib's
    built-in colormaps still work without this file. ``_reload`` only commits
    its result once the whole file has parsed and validated cleanly, so a bad
    entry can't leave the registry half-loaded from a previous, partial attempt.
    """
    if not _config_path.exists():
        logger.warning(
            "No colormaps.json found — starting with in-memory defaults only"
        )
        return
    try:
        data: dict[str, list | dict] = json.loads(_config_path.read_text())
        _reload(data)
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(
            f"Failed to load colormaps from {_config_path}: {e}. "
            "Starting with no custom colormaps (rio-tiler/matplotlib "
            "colormaps are still available)."
        )
        return
    logger.info(f"Loaded {len(_custom_colormaps)} colormaps from {_config_path}")


def list_colormaps() -> dict[str, list]:
    """Return all supported colormap names grouped by source.

    Priority mirrors _colormap(): custom → rio-tiler → matplotlib.
    Custom entries include their mode; rio-tiler and matplotlib entries are plain strings.
    """
    import matplotlib
    from rio_tiler.colormap import cmap as _rio_cmap

    custom = [
        {"name": name, "mode": _custom_colormap_modes.get(name, "ramp")}
        for name in _custom_colormaps
    ]
    custom_set = {entry["name"] for entry in custom}

    rio_names = sorted(n for n in _rio_cmap.list() if n not in custom_set)
    rio_set = set(rio_names)

    mpl_names = sorted(
        n for n in matplotlib.colormaps if n not in custom_set and n not in rio_set
    )

    return {"custom": custom, "rio_tiler": rio_names, "matplotlib": mpl_names}


def _reload(data: dict[str, list | dict]) -> None:
    """Validate and load every colormap, raising ``ValueError`` on the first bad one.

    Deliberately strict rather than best-effort: a malformed entry here would
    otherwise surface later as a wrong colour on a tile or a confusing crash at
    request time, far from the config line that caused it. Builds into local
    dicts and only commits them to the module-level registry at the end, so a
    bad entry partway through the file can't leave the registry half-loaded —
    callers (``load_colormaps``) see either the fully-loaded old-or-new state,
    or (on error) whatever was there before this call.
    """
    colormaps: dict[str, list[tuple[int, int, int, int]]] = {}
    modes: dict[str, ColormapMode] = {}
    category_values: dict[str, list[int]] = {}
    for name, value in data.items():
        if isinstance(value, dict):
            mode = _validate_mode(name, value)
            modes[name] = mode
            if mode == "categorical":
                colormaps[name], category_values[name] = _load_categorical_entry(
                    name, value
                )
            else:
                colormaps[name], category_values[name] = _load_ramp_entry(name, value)
        else:
            colormaps[name] = [
                tuple(_parse_rgba(name, c, i)) for i, c in enumerate(value)
            ]
    _custom_colormaps.clear()
    _custom_colormap_modes.clear()
    _custom_colormap_values.clear()
    _custom_colormaps.update(colormaps)
    _custom_colormap_modes.update(modes)
    _custom_colormap_values.update(category_values)


def _validate_mode(name: str, value: dict) -> ColormapMode:
    mode = value.get("mode")
    if mode not in ("ramp", "categorical"):
        raise ValueError(
            f"colormap {name!r}: 'mode' must be 'ramp' or 'categorical', got {mode!r}"
        )
    return mode


def _load_categorical_entry(
    name: str, value: dict
) -> tuple[list[tuple[int, int, int, int]], list[int]]:
    if "values" not in value or "colors" not in value:
        raise ValueError(
            f"colormap {name!r}: categorical mode requires 'values' and 'colors'"
        )
    values = value["values"]
    colors = value["colors"]
    if not isinstance(values, list) or not all(isinstance(v, int) for v in values):
        raise ValueError(f"colormap {name!r}: 'values' must be a list of integers")
    if len(values) != len(set(values)):
        raise ValueError(f"colormap {name!r}: 'values' must not contain duplicates")
    if any(not (0 <= v <= 255) for v in values):
        raise ValueError(
            f"colormap {name!r}: category values must be within 0-255 "
            "(rio-tiler's uint8 LUT can't represent codes outside that range)"
        )
    if len(values) != len(colors):
        raise ValueError(
            f"colormap {name!r}: 'values' has {len(values)} entries but 'colors' "
            f"has {len(colors)} — they must be the same length and order"
        )
    parsed_colors = [_parse_rgba(name, c, i) for i, c in enumerate(colors)]
    lut = [
        tuple(rgba) for rgba in categorical_lut(dict(zip(values, parsed_colors)))
    ]  # type: ignore[misc]
    return lut, values


def _load_ramp_entry(
    name: str, value: dict
) -> tuple[list[tuple[int, int, int, int]], list[int]]:
    if "entries" not in value:
        raise ValueError(f"colormap {name!r}: ramp mode requires 'entries'")
    entries = value["entries"]
    if len(entries) != 256:
        raise ValueError(
            f"colormap {name!r}: ramp 'entries' must have exactly 256 entries, "
            f"got {len(entries)}"
        )
    lut = [
        tuple(_parse_rgba(name, c, i)) for i, c in enumerate(entries)
    ]  # type: ignore[misc]
    return lut, list(value.get("values", []))


def _parse_rgba(name: str, color: object, index: int) -> list[int]:
    return parse_color(color, f"colormap {name!r} color at index {index}")
