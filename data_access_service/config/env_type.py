
from enum import Enum

class EnvType(Enum):
    DEV = "dev"
    TESTING = "testing"
    EDGE = "edge"
    STAGING = "staging"
    PRODUCTION = "prod"