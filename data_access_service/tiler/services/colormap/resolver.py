"""Look up a colormap by name: custom, then rio-tiler, then matplotlib."""

import numpy as np

from data_access_service.tiler.services.colormap.registry import get_colormap


def resolve_colormap(name: str) -> dict[int, tuple[int, int, int, int]]:
    """The rio-tiler colormap dict for ``name``."""
    from rio_tiler.colormap import cmap as _rio_cmap

    entries = get_colormap(name)
    if entries is not None:
        if len(entries) != 256:
            raise ValueError(
                f"Custom colormap {name!r} must have exactly 256 entries, got {len(entries)}"
            )
        return {i: entries[i] for i in range(256)}
    try:
        return _rio_cmap.get(name)
    except Exception:
        pass
    import matplotlib

    try:
        cm = matplotlib.colormaps[name]
    except KeyError as exc:
        raise ValueError(f"Unknown colormap: {name!r}") from exc
    rgba = (cm(np.linspace(0, 1, 256)) * 255).astype(np.uint8)
    return {
        i: (int(rgba[i, 0]), int(rgba[i, 1]), int(rgba[i, 2]), int(rgba[i, 3]))
        for i in range(256)
    }
