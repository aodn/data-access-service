"""Coordinate formatting for the download email. Mirrors the ogcapi-java EmailUtils."""

# Fixed number of decimal places for displayed coordinates (~1m precision).
COORDINATE_DECIMALS = 5


def format_coordinate(value) -> str:
    """Format a coordinate to a fixed number of decimal places, avoiding scientific notation."""
    return f"{float(value):.{COORDINATE_DECIMALS}f}"
