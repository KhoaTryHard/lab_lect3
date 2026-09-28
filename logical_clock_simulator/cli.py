# Cung cap giao dien dong lenh cho che do demo va random.
"""Command-line entry point."""

from __future__ import annotations

import argparse
import sys

from .simulator import Simulator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Three-process Lamport/vector clock simulator")
    parser.add_argument("--mode", choices=("demo", "random"), default="random")
    parser.add_argument("--steps", type=int, default=30, help="random attempts per worker")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--delay-ms", type=float, default=10.0)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--output-dir", default="output")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.steps < 0 or args.delay_ms < 0 or args.timeout_seconds <= 0:
        parser.error("steps and delay-ms must be non-negative; timeout-seconds must be positive")
    simulator = Simulator(seed=args.seed, timeout_seconds=args.timeout_seconds, console=True)
    try:
        if args.mode == "demo":
            summary = simulator.run_demo()
        else:
            summary = simulator.run_random(steps=args.steps, delay_ms=args.delay_ms)
        destination = simulator.write_outputs(args.output_dir)
        print(f"Summary written to {destination / 'summary.json'}")
        print(f"Events: {summary['event_count']} | Conflicts: {summary['conflicts']}")
        return 0
    except KeyboardInterrupt:
        try:
            simulator.stop()
        except Exception:
            pass
        print("Interrupted", file=sys.stderr)
        return 130
    except Exception as error:
        try:
            simulator.stop()
        except Exception:
            pass
        print(f"Simulation failed: {error}", file=sys.stderr)
        return 1
