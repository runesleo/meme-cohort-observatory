"""Coverage-aware lifecycle telemetry for newly observable token pools."""

from .adapters import PoolEvent, SourceBatch
from .cohort import Candidate, collect_once
from .store import CohortStore

__all__ = ["Candidate", "PoolEvent", "SourceBatch", "CohortStore", "collect_once"]
__version__ = "0.1.0"
