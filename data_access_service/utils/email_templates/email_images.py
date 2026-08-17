"""Pre-loaded base64-encoded images for email templates."""

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

IMG_DIR = Path(__file__).parent / "img"

_IMAGE_FILES = {
    "HEADER_IMG": "header.png",
    "BBOX_IMG": "bbox.png",
    "POLYGON_IMG": "polygon.png",
    "TIME_RANGE_IMG": "time-range.png",
    "ATTRIBUTES_IMG": "attributes.png",
    "FACEBOOK_IMG": "facebook.png",
    "INSTAGRAM_IMG": "instagram.png",
    "BLUESKY_IMG": "bluesky.png",
    "LINKEDIN_IMG": "linkedin.png",
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
BBOX_IMG = _results["BBOX_IMG"]
POLYGON_IMG = _results["POLYGON_IMG"]
TIME_RANGE_IMG = _results["TIME_RANGE_IMG"]
ATTRIBUTES_IMG = _results["ATTRIBUTES_IMG"]
FACEBOOK_IMG = _results["FACEBOOK_IMG"]
INSTAGRAM_IMG = _results["INSTAGRAM_IMG"]
BLUESKY_IMG = _results["BLUESKY_IMG"]
LINKEDIN_IMG = _results["LINKEDIN_IMG"]

# For convenience, export all as a dict too
EMAIL_IMAGES = _results

__all__ = [
    "HEADER_IMG",
    "BBOX_IMG",
    "POLYGON_IMG",
    "TIME_RANGE_IMG",
    "ATTRIBUTES_IMG",
    "FACEBOOK_IMG",
    "INSTAGRAM_IMG",
    "BLUESKY_IMG",
    "LINKEDIN_IMG",
]
