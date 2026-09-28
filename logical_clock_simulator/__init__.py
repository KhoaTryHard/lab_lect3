"""Three-process Lamport/vector clock simulator."""

from .core import (
    ClockEngine,
    ClockState,
    EventRecord,
    Message,
    VectorRelation,
    compare_vectors,
)
from .simulator import Simulator

__all__ = [
    "ClockEngine",
    "ClockState",
    "EventRecord",
    "Message",
    "Simulator",
    "VectorRelation",
    "compare_vectors",
]
