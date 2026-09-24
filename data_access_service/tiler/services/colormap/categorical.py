"""Colours for categorical variables (CF ``flag_values``): each code maps
to exactly one colour.

Values and labels come from the data. Colours come from, in order: a
categorical ``colormap=`` param, the variable's ``flag_colors`` attr, or
``DEFAULT_CATEGORICAL_PALETTE``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from data_access_service.tiler.services.colormap.registry import (
    get_colormap,
    is_categorical,
)
from data_access_service.tiler.utils.colors import parse_color

RGBA = tuple[int, int, int, int]

# Matched to flag_values by position, cycled if there are more categories.
# Marine cold-spell colours (Hobday et al. 2018); 0 ("none") is transparent.
DEFAULT_CATEGORICAL_PALETTE: list[RGBA] = [
    (0, 0, 0, 0),  # 0 none — transparent
    (199, 236, 242, 255),  # 1 moderate  #C7ECF2
    (133, 183, 204, 255),  # 2 strong    #85B7CC
    (74, 111, 167, 255),  # 3 severe    #4A6FA7
    (17, 30, 108, 255),  # 4 extreme   #111E6C
]


def is_categorical_variable(attrs: Mapping[str, Any]) -> bool:
    """True if the variable has CF ``flag_values``."""
    return attrs.get("flag_values") is not None


@dataclass(frozen=True)
class CategoricalScheme:
    """Value -> colour for one categorical variable."""

    values: tuple[int, ...]
    colors: tuple[RGBA, ...]
    labels: tuple[str, ...] | None

    def lut(self) -> dict[int, RGBA]:
        """A 256-entry LUT indexed by code; others transparent, codes outside
        0-255 skipped."""
        table: dict[int, RGBA] = {i: (0, 0, 0, 0) for i in range(256)}
        for value, color in zip(self.values, self.colors, strict=False):
            if 0 <= value <= 255:
                table[value] = color
        return table


def parse_flag_values_and_meanings(
    attrs: Mapping[str, Any],
) -> tuple[tuple[int, ...], tuple[str, ...] | None]:
    """``(flag_values, flag_meanings)`` from the attrs; labels are None if
    missing or not one per value."""
    values = tuple(_as_int_list(attrs.get("flag_values")))
    labels = _parse_meanings(attrs.get("flag_meanings"), len(values))
    return values, labels


def resolve_scheme(
    attrs: Mapping[str, Any], colormap_name: str | None
) -> CategoricalScheme:
    """The scheme for a categorical variable (see the module docstring)."""
    values, labels = parse_flag_values_and_meanings(attrs)
    colors = _resolve_colors(values, attrs, colormap_name)
    return CategoricalScheme(values=values, colors=colors, labels=labels)


def _resolve_colors(
    values: tuple[int, ...],
    attrs: Mapping[str, Any],
    colormap_name: str | None,
) -> tuple[RGBA, ...]:
    n = len(values)

    # 1. A categorical colormap param (already checked to match the values).
    if colormap_name and is_categorical(colormap_name):
        explicit = _registered_categorical_colors(colormap_name, values)
        if explicit:
            return tuple(explicit)

    # 2. Colours in the data.
    flag_colors = attrs.get("flag_colors")
    if flag_colors:
        parsed = _parse_flag_colors(flag_colors)
        if parsed:
            return _fit(parsed, n)

    # 3. The default palette.
    return _fit(DEFAULT_CATEGORICAL_PALETTE, n)


def _registered_categorical_colors(name: str, values: tuple[int, ...]) -> list[RGBA]:
    """Each value's colour from a categorical colormap (stored at its own slot)."""
    lut = get_colormap(name)
    if not lut or not values:
        return []
    return [tuple(lut[v]) if 0 <= v <= 255 else (0, 0, 0, 0) for v in values]  # type: ignore[misc]


def _parse_flag_colors(raw: Any) -> list[RGBA]:
    """Parse ``flag_colors``, skipping entries that don't parse."""
    items: Sequence[Any]
    if isinstance(raw, str):
        items = raw.split()
    elif isinstance(raw, Sequence):
        items = raw
    else:
        return []
    out: list[RGBA] = []
    for item in items:
        try:
            r, g, b, a = parse_color(item, "flag_colors")
            out.append((r, g, b, a))
        except ValueError:
            continue
    return out


def _fit(colors: Sequence[RGBA], n: int) -> tuple[RGBA, ...]:
    """Exactly n colours, cycling if needed."""
    if not colors:
        colors = DEFAULT_CATEGORICAL_PALETTE
    return tuple(colors[i % len(colors)] for i in range(n))


def _parse_meanings(raw: Any, n: int) -> tuple[str, ...] | None:
    """One label per value, or None."""
    if raw is None:
        return None
    labels = raw.split() if isinstance(raw, str) else [str(x) for x in raw]
    if len(labels) != n:
        return None  # don't mispair labels
    return tuple(labels)


def _as_int_list(raw: Any) -> list[int]:
    """flag_values as a list of ints."""
    if raw is None or isinstance(raw, str | bytes):
        return []
    values = raw.tolist() if hasattr(raw, "tolist") else raw
    if not isinstance(values, list | tuple):
        values = [values]
    out: list[int] = []
    for v in values:
        try:
            out.append(int(v))
        except (ValueError, TypeError):
            continue
    return out
