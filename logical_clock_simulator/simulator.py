# Quan ly ba worker, queue message va log cua bo mo phong.
"""Threaded three-process simulator and structured logging."""

from __future__ import annotations

import json
import queue
import random
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, TextIO

from .core import ClockEngine, EventRecord, Message, VectorRelation


@dataclass(frozen=True)
class Command:
    command_id: str
    kind: str
    target: Optional[int] = None
    steps: int = 0
    delay_seconds: float = 0.0


@dataclass
class WorkerOutput:
    worker_id: int
    command_id: str
    output_type: str
    event: Optional[EventRecord] = None
    phase: Optional[str] = None
    payload: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


class Worker(threading.Thread):
    """One simulated process.  Its engine and counters never leave this thread."""

    def __init__(
        self,
        pid: int,
        command_queue: queue.Queue[Command],
        inboxes: list[queue.Queue[Message]],
        output_queue: queue.Queue[WorkerOutput],
        seed: int,
    ) -> None:
        super().__init__(name=f"P{pid}", daemon=False)
        self.pid = pid
        self.command_queue = command_queue
        self.inboxes = inboxes
        self.output_queue = output_queue
        self.engine = ClockEngine(pid)
        self.rng = random.Random(seed + pid)
        self._event_number = 0
        self._message_number = 0

    def _next_event_id(self) -> str:
        self._event_number += 1
        return f"P{self.pid}-e{self._event_number}"

    def _next_message_id(self) -> str:
        self._message_number += 1
        return f"P{self.pid}-m{self._message_number}"

    def _event(self, command_id: str, event: EventRecord) -> None:
        self.output_queue.put(
            WorkerOutput(worker_id=self.pid, command_id=command_id, output_type="event", event=event)
        )

    def _status(self, command_id: str, phase: str, **payload: Any) -> None:
        self.output_queue.put(
            WorkerOutput(
                worker_id=self.pid,
                command_id=command_id,
                output_type="status",
                phase=phase,
                payload=payload,
            )
        )

    def _error(self, command_id: str, error: Exception) -> None:
        self.output_queue.put(
            WorkerOutput(
                worker_id=self.pid,
                command_id=command_id,
                output_type="status",
                phase="error",
                error=f"{type(error).__name__}: {error}",
            )
        )

    def _perform_local(self, command_id: str) -> None:
        self._event(command_id, self.engine.local(self._next_event_id()))

    def _perform_send(self, command_id: str, target: int) -> None:
        message, event = self.engine.send(self._next_event_id(), target, self._next_message_id())
        # The trace is published before the message becomes visible to the receiver.
        self._event(command_id, event)
        self.inboxes[target].put(message)

    def _perform_receive(self, command_id: str) -> None:
        try:
            message = self.inboxes[self.pid].get_nowait()
        except queue.Empty:
            self._event(command_id, self.engine.receive_skipped(self._next_event_id()))
            return
        self._event(command_id, self.engine.receive(self._next_event_id(), message))

    def _perform_random(self, command_id: str, steps: int, delay_seconds: float) -> None:
        if steps < 0:
            raise ValueError("steps must be non-negative")
        for _ in range(steps):
            action = self.rng.choice(("LOCAL", "SEND", "RECEIVE"))
            if action == "LOCAL":
                self._perform_local(command_id)
            elif action == "SEND":
                targets = [pid for pid in range(len(self.inboxes)) if pid != self.pid]
                self._perform_send(command_id, self.rng.choice(targets))
            else:
                self._perform_receive(command_id)
            if delay_seconds:
                time.sleep(delay_seconds)

    def _drain(self, command_id: str) -> int:
        received = 0
        while True:
            try:
                message = self.inboxes[self.pid].get_nowait()
            except queue.Empty:
                return received
            self._event(command_id, self.engine.receive(self._next_event_id(), message))
            received += 1

    def _clock_payload(self) -> dict[str, Any]:
        lamport, vector = self.engine.state.snapshot()
        return {"lamport": lamport, "vector": list(vector)}

    def _run_command(self, command: Command) -> bool:
        if command.kind == "LOCAL":
            self._perform_local(command.command_id)
        elif command.kind == "SEND":
            if command.target is None:
                raise ValueError("SEND requires a target")
            self._perform_send(command.command_id, command.target)
        elif command.kind == "RECEIVE":
            self._perform_receive(command.command_id)
        elif command.kind == "RUN_RANDOM":
            self._perform_random(command.command_id, command.steps, command.delay_seconds)
        elif command.kind == "DRAIN":
            drained = self._drain(command.command_id)
            self._status(command.command_id, "drained", drained=drained, **self._clock_payload())
            return True
        elif command.kind == "STOP":
            self._status(command.command_id, "stopped", **self._clock_payload())
            return False
        else:
            raise ValueError(f"unknown command: {command.kind}")
        self._status(command.command_id, "done", **self._clock_payload())
        return True

    def run(self) -> None:
        while True:
            command = self.command_queue.get()
            try:
                keep_running = self._run_command(command)
            except Exception as error:  # report failures to the manager, never silently lose them
                self._error(command.command_id, error)
                keep_running = True
            if not keep_running:
                return


def format_event(event: EventRecord) -> list[str]:
    vector = list(event.vector_after)
    if event.event_type == "LOCAL":
        return [f"[P{event.pid}] LOCAL EVENT   | Lamport: {event.lamport_after} | Vector: {vector}"]
    if event.event_type == "SEND":
        return [
            f"[P{event.pid}] SEND MSG to P{event.peer} | Lamport: {event.lamport_after} | Vector: {vector}"
        ]
    if event.event_type == "RECV_SKIPPED":
        return [f"[P{event.pid}] RECV SKIPPED   | Lamport: {event.lamport_after} | Vector: {vector}"]
    lines: list[str] = []
    if event.conflict:
        lines.append(
            f"[P{event.pid}] CONFLICT DETECTED with P{event.peer}! "
            f"Vector {list(event.vector_before)} || {list(event.message_vector or ())}"
        )
    lines.append(
        f"[P{event.pid}] RECV MSG fr P{event.peer} | Lamport: {event.lamport_after} | Vector: {vector}"
    )
    return lines


class Simulator:
    """Own queues, worker lifecycle, event collection and output files."""

    def __init__(
        self,
        *,
        seed: int = 42,
        timeout_seconds: float = 30.0,
        console: bool = True,
        stream: Optional[TextIO] = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.seed = seed
        self.timeout_seconds = timeout_seconds
        self.console = console
        self.stream = stream
        self.inboxes: list[queue.Queue[Message]] = [queue.Queue() for _ in range(3)]
        self.command_queues: list[queue.Queue[Command]] = [queue.Queue() for _ in range(3)]
        self.output_queue: queue.Queue[WorkerOutput] = queue.Queue()
        self.workers = [
            Worker(pid, self.command_queues[pid], self.inboxes, self.output_queue, seed)
            for pid in range(3)
        ]
        self.events: list[EventRecord] = []
        self.log_lines: list[str] = []
        self.final_clocks: dict[int, dict[str, Any]] = {}
        self._pending: dict[str, list[WorkerOutput]] = {}
        self._command_number = 0
        self.started = False
        self.stopped = False

    def start(self) -> None:
        if self.started:
            return
        for worker in self.workers:
            worker.start()
        self.started = True

    def _next_command_id(self) -> str:
        self._command_number += 1
        return f"cmd-{self._command_number}"

    def _dispatch(self, pid: int, kind: str, *, target: Optional[int] = None, steps: int = 0, delay_seconds: float = 0.0) -> str:
        if not self.started:
            raise RuntimeError("simulator has not been started")
        command_id = self._next_command_id()
        self.command_queues[pid].put(
            Command(command_id, kind, target=target, steps=steps, delay_seconds=delay_seconds)
        )
        return command_id

    def _record_output(self, output: WorkerOutput) -> None:
        if output.output_type == "event" and output.event is not None:
            self.events.append(output.event)
            lines = format_event(output.event)
            self.log_lines.extend(lines)
            if self.console:
                destination = self.stream
                for line in lines:
                    if destination is None:
                        print(line)
                    else:
                        print(line, file=destination)
        elif output.output_type == "status" and output.phase in {"drained", "stopped"}:
            self.final_clocks[output.worker_id] = {
                "lamport": output.payload.get("lamport"),
                "vector": output.payload.get("vector"),
            }

    def _pending_status(self, command_id: str) -> Optional[WorkerOutput]:
        entries = self._pending.get(command_id, [])
        for index, output in enumerate(entries):
            if output.output_type == "status":
                entries.pop(index)
                if not entries:
                    self._pending.pop(command_id, None)
                return output
        return None

    def _wait_for(self, command_id: str) -> WorkerOutput:
        deadline = time.monotonic() + self.timeout_seconds
        pending = self._pending_status(command_id)
        if pending is not None:
            if pending.phase == "error":
                raise RuntimeError(pending.error or "worker command failed")
            return pending
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"timed out waiting for {command_id}")
            try:
                output = self.output_queue.get(timeout=remaining)
            except queue.Empty as error:
                raise TimeoutError(f"timed out waiting for {command_id}") from error
            self._record_output(output)
            if output.command_id != command_id:
                self._pending.setdefault(output.command_id, []).append(output)
                continue
            if output.output_type == "status":
                if output.phase == "error":
                    raise RuntimeError(output.error or "worker command failed")
                return output

    def _dispatch_wait(self, pid: int, kind: str, **kwargs: Any) -> WorkerOutput:
        return self._wait_for(self._dispatch(pid, kind, **kwargs))

    def run_demo(self) -> dict[str, Any]:
        self.start()
        try:
            self._dispatch_wait(0, "LOCAL")
            self._dispatch_wait(0, "SEND", target=1)
            self._dispatch_wait(1, "RECEIVE")
            self._dispatch_wait(2, "LOCAL")
            self._dispatch_wait(1, "SEND", target=2)
            self._dispatch_wait(2, "RECEIVE")
            self.drain()
        finally:
            self.stop()
        return self.summary()

    def run_random(self, steps: int = 30, delay_ms: float = 10.0) -> dict[str, Any]:
        if steps < 0:
            raise ValueError("steps must be non-negative")
        if delay_ms < 0:
            raise ValueError("delay_ms must be non-negative")
        self.start()
        try:
            commands = [
                self._dispatch(pid, "RUN_RANDOM", steps=steps, delay_seconds=delay_ms / 1000.0)
                for pid in range(3)
            ]
            for command_id in commands:
                self._wait_for(command_id)
            self.drain()
        finally:
            self.stop()
        return self.summary()

    def drain(self) -> None:
        commands = [self._dispatch(pid, "DRAIN") for pid in range(3)]
        for command_id in commands:
            self._wait_for(command_id)

    def stop(self) -> None:
        if not self.started or self.stopped:
            return
        errors: list[Exception] = []
        try:
            commands = [self._dispatch(pid, "STOP") for pid in range(3)]
            for command_id in commands:
                try:
                    self._wait_for(command_id)
                except Exception as error:
                    errors.append(error)
        finally:
            for worker in self.workers:
                worker.join(timeout=self.timeout_seconds)
            self.stopped = True
        if errors:
            raise errors[0]

    def summary(self) -> dict[str, Any]:
        counts = {"LOCAL": 0, "SEND": 0, "RECEIVE": 0, "RECV_SKIPPED": 0, "CONFLICT": 0}
        for event in self.events:
            counts[event.event_type] = counts.get(event.event_type, 0) + 1
            if event.conflict:
                counts["CONFLICT"] += 1
        return {
            "seed": self.seed,
            "event_count": len(self.events),
            "counts": counts,
            "conflicts": counts["CONFLICT"],
            "final_clocks": {str(pid): self.final_clocks.get(pid) for pid in range(3)},
            "workers_alive": [worker.is_alive() for worker in self.workers],
        }

    def write_outputs(self, output_dir: str | Path) -> Path:
        destination = Path(output_dir)
        destination.mkdir(parents=True, exist_ok=True)
        (destination / "events.log").write_text("\n".join(self.log_lines) + ("\n" if self.log_lines else ""), encoding="utf-8")
        with (destination / "events.jsonl").open("w", encoding="utf-8") as handle:
            for event in self.events:
                handle.write(json.dumps(event.to_dict(), ensure_ascii=False) + "\n")
        (destination / "summary.json").write_text(
            json.dumps(self.summary(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        return destination
