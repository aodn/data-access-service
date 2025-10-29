"""Pre-loaded base64-encoded images for email templates."""

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

IMG_DIR = Path(__file__).parent / "img"

_IMAGE_FILES = {
    "HEADER_IMG": "a0239805d37afe34cc5372f3ff2a0d2d.png",
    "DOWNLOAD_ICON": "e58a6cfdb179246fe86eb69b984294cf.png",
    "BBOX_IMG": "1419180c04931cd1e419efce2690d5b8.png",
    "TIME_RANGE_IMG": "6a9d0202f46342bc6bc7c3998cfa42b7.png",
    "ATTRIBUTES_IMG": "b274dfda8abcd8b25fba336b3005aea1.png",
    "FACEBOOK_IMG": "61b8da3fc7dfb3c319f6ae1e199a7e21.png",
    "INSTAGRAM_IMG": "61d7bf2cff5309382bd988da59d406db.png",
    "X_IMG": "93c45889ccd4dbea02f449acff11cfb1.png",
    "BLUESKY_IMG": "e2ae815b8ce719a4026e08f7fb334b71.png",
    "YOUTUBE_IMG": "91912da1b4494c1dff3048977473c402.png",
    "CONTACT_IMG": "44a03adf2a462ad89ad6e3e05760ecb1.png",
    "LINKEDIN_IMG": "82192244cf3677674fda6af8c471897d.png",
}


def _load_image(item):
    """Load a single image and return (name, base64_data)"""
    from data_access_service.utils.email_templates.png_to_base64 import (
        png_to_base64,
    )  # Import here to avoid circular imports

    name, filename = item
    return name, png_to_base64(IMG_DIR / filename)


# Load all images in parallel at module import time
with ThreadPoolExecutor(max_workers=10) as executor:
    _results = dict(executor.map(_load_image, _IMAGE_FILES.items()))

# Export as module-level constants
HEADER_IMG = _results["HEADER_IMG"]
DOWNLOAD_ICON = _results["DOWNLOAD_ICON"]
BBOX_IMG = _results["BBOX_IMG"]
TIME_RANGE_IMG = _results["TIME_RANGE_IMG"]
ATTRIBUTES_IMG = _results["ATTRIBUTES_IMG"]
FACEBOOK_IMG = _results["FACEBOOK_IMG"]
INSTAGRAM_IMG = _results["INSTAGRAM_IMG"]
X_IMG = _results["X_IMG"]
BLUESKY_IMG=_results["BLUESKY_IMG"]
YOUTUBE_IMG = _results["YOUTUBE_IMG"]
CONTACT_IMG = _results["CONTACT_IMG"]
LINKEDIN_IMG = _results["LINKEDIN_IMG"]

# For convenience, export all as a dict too
EMAIL_IMAGES = _results

__all__ = [
    "HEADER_IMG",
    "DOWNLOAD_ICON",
    "BBOX_IMG",
    "TIME_RANGE_IMG",
    "ATTRIBUTES_IMG",
    "FACEBOOK_IMG",
    "INSTAGRAM_IMG",
    "X_IMG",
    "BLUESKY_IMG"
    "YOUTUBE_IMG",
    "CONTACT_IMG",
    "EMAIL_IMAGES",
    "LINKEDIN_IMG",
]
