import unittest

from logical_clock_simulator.core import (
    ClockEngine,
    ClockState,
    Message,
    VectorRelation,
    compare_vectors,
)


class VectorComparisonTests(unittest.TestCase):
    def test_all_relations_are_component_wise(self):
        self.assertIs(compare_vectors([1, 0, 0], [2, 1, 0]), VectorRelation.BEFORE)
        self.assertIs(compare_vectors([2, 1, 0], [1, 0, 0]), VectorRelation.AFTER)
        self.assertIs(compare_vectors([1, 2, 3], [1, 2, 3]), VectorRelation.EQUAL)
        self.assertIs(compare_vectors([2, 1, 3], [2, 2, 2]), VectorRelation.CONCURRENT)

    def test_lexicographic_order_is_not_used(self):
        self.assertIs(compare_vectors([0, 9, 0], [1, 0, 0]), VectorRelation.CONCURRENT)


class ClockEngineTests(unittest.TestCase):
    def test_local_and_send_increment_once(self):
        engine = ClockEngine(0)
        local = engine.local("e1")
        self.assertEqual((local.lamport_after, local.vector_after), (1, (1, 0, 0)))
        message, send = engine.send("e2", 1, "m1")
        self.assertEqual((send.lamport_after, send.vector_after), (2, (2, 0, 0)))
        self.assertEqual(message.vector, (2, 0, 0))

    def test_receive_merges_then_increments_receiver_component(self):
        engine = ClockEngine(1, ClockState(lamport=7, vector=[0, 4, 1]))
        message = Message("m1", sender=0, receiver=1, lamport=12, vector=(3, 2, 9))
        event = engine.receive("e1", message)
        self.assertEqual(event.lamport_after, 13)
        self.assertEqual(event.vector_after, (3, 5, 9))
        self.assertEqual(event.relation, VectorRelation.CONCURRENT)
        self.assertTrue(event.conflict)

    def test_receive_does_not_report_causal_message_as_conflict(self):
        engine = ClockEngine(1, ClockState(lamport=1, vector=[1, 1, 0]))
        message = Message("m1", sender=0, receiver=1, lamport=2, vector=(1, 0, 0))
        event = engine.receive("e1", message)
        self.assertEqual(event.relation, VectorRelation.AFTER)
        self.assertFalse(event.conflict)

    def test_empty_receive_does_not_change_clock(self):
        engine = ClockEngine(2)
        event = engine.receive_skipped("e1")
        self.assertTrue(event.skipped)
        self.assertEqual(event.lamport_before, event.lamport_after)
        self.assertEqual(event.vector_before, event.vector_after)


if __name__ == "__main__":
    unittest.main()
