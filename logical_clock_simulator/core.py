"""Pure logical-clock mechanics used by the worker threads and tests.

The engine deliberately owns one mutable clock state.  A simulator creates one
engine per worker, so no clock state is shared between processes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable, Optional

PROCESS_COUNT = 3
Vector = tuple[int, ...]


class VectorRelation(str, Enum):
    BEFORE = "BEFORE"
    AFTER = "AFTER"
    EQUAL = "EQUAL"
    CONCURRENT = "CONCURRENT"


def _as_vector(values: Iterable[int], *, size: Optional[int] = None) -> Vector:
    result = tuple(int(value) for value in values)
    if size is not None and len(result) != size:
        raise ValueError(f"vector must have length {size}, got {len(result)}")
    if any(value < 0 for value in result):
        raise ValueError("vector components must be non-negative")
    return result


def compare_vectors(left: Iterable[int], right: Iterable[int]) -> VectorRelation:
    """Compare vectors component-wise, never lexicographically."""

    a = _as_vector(left)
    b = _as_vector(right)
    if len(a) != len(b):
        raise ValueError("vectors must have the same length")
    a_le_b = all(x <= y for x, y in zip(a, b))
    b_le_a = all(y <= x for x, y in zip(a, b))
    if a_le_b and b_le_a:
        return VectorRelation.EQUAL
    if a_le_b:
        return VectorRelation.BEFORE
    if b_le_a:
        return VectorRelation.AFTER
    return VectorRelation.CONCURRENT


@dataclass
class ClockState:
    """Mutable state private to a single worker."""

    lamport: int = 0
    vector: list[int] = field(default_factory=lambda: [0] * PROCESS_COUNT)

    def __post_init__(self) -> None:
        if self.lamport < 0:
            raise ValueError("Lamport clock must be non-negative")
        if len(self.vector) != PROCESS_COUNT:
            raise ValueError(f"vector must have length {PROCESS_COUNT}")
        if any(value < 0 for value in self.vector):
            raise ValueError("vector components must be non-negative")

    def snapshot(self) -> tuple[int, Vector]:
        return self.lamport, tuple(self.vector)


@dataclass(frozen=True)
class Message:
    message_id: str
    sender: int
    receiver: int
    lamport: int
    vector: Vector

    def __post_init__(self) -> None:
        if not 0 <= self.sender < PROCESS_COUNT:
            raise ValueError("invalid message sender")
        if not 0 <= self.receiver < PROCESS_COUNT:
            raise ValueError("invalid message receiver")
        if self.sender == self.receiver:
            raise ValueError("a message target must differ from its sender")
        if self.lamport < 0:
            raise ValueError("message Lamport timestamp must be non-negative")
        object.__setattr__(self, "vector", _as_vector(self.vector, size=PROCESS_COUNT))


@dataclass(frozen=True)
class EventRecord:
    event_id: str
    pid: int
    event_type: str
    lamport_before: int
    lamport_after: int
    vector_before: Vector
    vector_after: Vector
    peer: Optional[int] = None
    message_id: Optional[str] = None
    message_lamport: Optional[int] = None
    message_vector: Optional[Vector] = None
    relation: Optional[VectorRelation] = None
    conflict: bool = False
    skipped: bool = False

    def __post_init__(self) -> None:
        if not 0 <= self.pid < PROCESS_COUNT:
            raise ValueError("invalid event process id")
        object.__setattr__(self, "vector_before", _as_vector(self.vector_before, size=PROCESS_COUNT))
        object.__setattr__(self, "vector_after", _as_vector(self.vector_after, size=PROCESS_COUNT))
        if self.message_vector is not None:
            object.__setattr__(self, "message_vector", _as_vector(self.message_vector, size=PROCESS_COUNT))

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "pid": self.pid,
            "event_type": self.event_type,
            "lamport_before": self.lamport_before,
            "lamport_after": self.lamport_after,
            "vector_before": list(self.vector_before),
            "vector_after": list(self.vector_after),
            "peer": self.peer,
            "message_id": self.message_id,
            "message_lamport": self.message_lamport,
            "message_vector": list(self.message_vector) if self.message_vector is not None else None,
            "relation": self.relation.value if self.relation is not None else None,
            "conflict": self.conflict,
            "skipped": self.skipped,
        }


class ClockEngine:
    """Apply Lamport and vector-clock rules for one process."""

    def __init__(self, pid: int, state: Optional[ClockState] = None) -> None:
        if not 0 <= pid < PROCESS_COUNT:
            raise ValueError("pid must be 0, 1, or 2")
        self.pid = pid
        self.state = state or ClockState()

    def _local_increment(self) -> tuple[int, Vector, int, Vector]:
        before_lamport, before_vector = self.state.snapshot()
        self.state.lamport += 1
        self.state.vector[self.pid] += 1
        after_lamport, after_vector = self.state.snapshot()
        return before_lamport, before_vector, after_lamport, after_vector

    def local(self, event_id: str) -> EventRecord:
        before_l, before_v, after_l, after_v = self._local_increment()
        return EventRecord(
            event_id=event_id,
            pid=self.pid,
            event_type="LOCAL",
            lamport_before=before_l,
            lamport_after=after_l,
            vector_before=before_v,
            vector_after=after_v,
        )

    def send(self, event_id: str, target: int, message_id: str) -> tuple[Message, EventRecord]:
        if not 0 <= target < PROCESS_COUNT or target == self.pid:
            raise ValueError("send target must be another valid process")
        before_l, before_v, after_l, after_v = self._local_increment()
        message = Message(
            message_id=message_id,
            sender=self.pid,
            receiver=target,
            lamport=after_l,
            vector=after_v,
        )
        event = EventRecord(
            event_id=event_id,
            pid=self.pid,
            event_type="SEND",
            lamport_before=before_l,
            lamport_after=after_l,
            vector_before=before_v,
            vector_after=after_v,
            peer=target,
            message_id=message_id,
            message_lamport=message.lamport,
            message_vector=message.vector,
        )
        return message, event

    def receive(self, event_id: str, message: Message) -> EventRecord:
        if message.receiver != self.pid:
            raise ValueError(f"message {message.message_id} targets P{message.receiver}, not P{self.pid}")
        before_l, before_v = self.state.snapshot()
        relation = compare_vectors(before_v, message.vector)
        conflict = relation is VectorRelation.CONCURRENT

        self.state.lamport = max(self.state.lamport, message.lamport) + 1
        self.state.vector = [max(local, incoming) for local, incoming in zip(self.state.vector, message.vector)]
        self.state.vector[self.pid] += 1
        after_l, after_v = self.state.snapshot()
        return EventRecord(
            event_id=event_id,
            pid=self.pid,
            event_type="RECEIVE",
            lamport_before=before_l,
            lamport_after=after_l,
            vector_before=before_v,
            vector_after=after_v,
            peer=message.sender,
            message_id=message.message_id,
            message_lamport=message.lamport,
            message_vector=message.vector,
            relation=relation,
            conflict=conflict,
        )

    def receive_skipped(self, event_id: str) -> EventRecord:
        lamport, vector = self.state.snapshot()
        return EventRecord(
            event_id=event_id,
            pid=self.pid,
            event_type="RECV_SKIPPED",
            lamport_before=lamport,
            lamport_after=lamport,
            vector_before=vector,
            vector_after=vector,
            skipped=True,
        )
