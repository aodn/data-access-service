from dataclasses import dataclass
from datetime import datetime
from http import HTTPStatus
from typing import Optional


@dataclass
class ErrorResponse:
    status_code: HTTPStatus
    timestamp: datetime
    details: Optional[str]
    parameters: Optional[str]

    def __init__(self,
                 status_code: HTTPStatus = HTTPStatus.OK,
                 timestamp: datetime = datetime.now(),
                 details: str = None,
                 parameters: str = None):

        self.status_code = status_code
        self.timestamp = timestamp
        self.details = details
        self.parameters = parameters
