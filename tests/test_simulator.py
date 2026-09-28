import json
import tempfile
import unittest
from pathlib import Path

from logical_clock_simulator.simulator import Simulator


class SimulatorIntegrationTests(unittest.TestCase):
    def test_demo_matches_spec_and_stops_all_workers(self):
        simulator = Simulator(seed=42, timeout_seconds=5, console=False)
        summary = simulator.run_demo()
        meaningful = [event for event in simulator.events if not event.skipped]
        self.assertEqual(
            [(event.pid, event.event_type, event.lamport_after, event.vector_after) for event in meaningful],
            [
                (0, "LOCAL", 1, (1, 0, 0)),
                (0, "SEND", 2, (2, 0, 0)),
                (1, "RECEIVE", 3, (2, 1, 0)),
                (2, "LOCAL", 1, (0, 0, 1)),
                (1, "SEND", 4, (2, 2, 0)),
                (2, "RECEIVE", 5, (2, 2, 2)),
            ],
        )
        self.assertEqual(summary["conflicts"], 1)
        self.assertEqual(summary["final_clocks"]["0"], {"lamport": 2, "vector": [2, 0, 0]})
        self.assertEqual(summary["final_clocks"]["1"], {"lamport": 4, "vector": [2, 2, 0]})
        self.assertEqual(summary["final_clocks"]["2"], {"lamport": 5, "vector": [2, 2, 2]})
        self.assertEqual(summary["workers_alive"], [False, False, False])

    def test_random_run_drains_messages_and_writes_structured_logs(self):
        simulator = Simulator(seed=7, timeout_seconds=5, console=False)
        summary = simulator.run_random(steps=100, delay_ms=0)
        self.assertEqual(summary["workers_alive"], [False, False, False])
        self.assertEqual(summary["event_count"], len(simulator.events))
        self.assertGreater(summary["counts"]["SEND"], 0)
        with tempfile.TemporaryDirectory() as directory:
            path = simulator.write_outputs(directory)
            self.assertTrue((path / "events.log").exists())
            self.assertTrue((path / "events.jsonl").exists())
            self.assertTrue((path / "summary.json").exists())
            rows = [json.loads(line) for line in (path / "events.jsonl").read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(rows), len(simulator.events))
            self.assertEqual(json.loads((path / "summary.json").read_text(encoding="utf-8"))["event_count"], len(rows))

    def test_random_seeds_preserve_clock_invariants(self):
        for seed in range(10):
            simulator = Simulator(seed=seed, timeout_seconds=5, console=False)
            simulator.run_random(steps=100, delay_ms=0)
            for event in simulator.events:
                if event.event_type == "RECV_SKIPPED":
                    self.assertEqual(event.lamport_before, event.lamport_after)
                    self.assertEqual(event.vector_before, event.vector_after)
                    continue
                if event.event_type in {"LOCAL", "SEND"}:
                    self.assertEqual(event.lamport_after, event.lamport_before + 1)
                    for index, (before, after) in enumerate(zip(event.vector_before, event.vector_after)):
                        expected = before + (1 if index == event.pid else 0)
                        self.assertEqual(after, expected)
                    continue
                self.assertEqual(event.event_type, "RECEIVE")
                self.assertEqual(event.lamport_after, max(event.lamport_before, event.message_lamport) + 1)
                self.assertIsNotNone(event.message_vector)
                for index, (before, incoming, after) in enumerate(
                    zip(event.vector_before, event.message_vector, event.vector_after)
                ):
                    expected = max(before, incoming) + (1 if index == event.pid else 0)
                    self.assertEqual(after, expected)

    def test_inbox_receive_is_exactly_once_after_drain(self):
        simulator = Simulator(seed=1, timeout_seconds=5, console=False)
        simulator.start()
        try:
            simulator._dispatch_wait(0, "LOCAL")
            simulator._dispatch_wait(0, "SEND", target=1)
            simulator.drain()
        finally:
            simulator.stop()
        receives = [event for event in simulator.events if event.event_type == "RECEIVE"]
        self.assertEqual(len(receives), 1)
        self.assertEqual(simulator.inboxes[1].qsize(), 0)


if __name__ == "__main__":
    unittest.main()
