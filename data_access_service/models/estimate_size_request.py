from typing import List, Optional, Union
from pydantic import BaseModel
from data_access_service.models.subset_request import NON_SPECIFIED


class EstimateSizeRequest(BaseModel):
    keys: Optional[List[str]] = None
    start_date: Optional[str] = NON_SPECIFIED
    end_date: Optional[str] = NON_SPECIFIED
    columns: Optional[List[str]] = None
    # Accept a GeoJSON MultiPolygon as either an inline object or a string.
    multi_polygon: Optional[Union[str, dict]] = None
    output_format: str = "netcdf"
