"""On-disk file/directory paths the server reads at startup.

Separated from [[constants]] (which holds the server↔shader contract values)
because paths are operational config, not shader-coupled invariants. Changing a
path doesn't risk silently corrupting tile output.
"""

from pathlib import Path

# Products and colormaps are static config committed with the package (edit +
# redeploy to change) — same static-asset rationale as the mask paths below,
# so they're resolved relative to the package rather than the CWD.
PRODUCTS_CONFIG_PATH = str(Path(__file__).resolve().parent / "products.json")
COLORMAPS_CONFIG_PATH = str(Path(__file__).resolve().parent / "colormaps.json")

# Mask assets stay with the tiler package (not config) since they're binary
# data, not something a dev edits — resolved relative to the repo's
# data_access_service package root so this keeps working regardless of where
# this config module itself lives.
_TILER_ASSETS = Path(__file__).resolve().parents[2] / "tiler" / "assets"
# Committed global land-mask asset for coastal fill (see services/rendering/masks.py).
# Regenerate with scripts/build_land_mask.py.
LAND_MASK_PATH = str(_TILER_ASSETS / "land_mask.npz")
# Committed regional ocean-validity mask (see services/rendering/masks.py).
# Regenerate with scripts/build_ocean_mask.py from src/app/assets/OCmask.nc.
OCEAN_MASK_PATH = str(_TILER_ASSETS / "ocean_mask.npz")
