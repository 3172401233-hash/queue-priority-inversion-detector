# -*- coding: utf-8 -*-
"""Unit tests for the queue priority-inversion detector."""

import unittest

from inversion_detector import (
    Batch,
    Message,
    PriorityInversionDetector,
    SchedulerConfig,
)


def make_config(capacity=2, weights=None, horizon=12):
    return SchedulerConfig(
        consumer_capacity=capacity,
        fairness_weights=weights or {1: 1.0, 10: 1.0},
        horizon_rounds=horizon,
    )


class BatchBlockingTest(unittest.TestCase):
    """A low-priority batch occupying every consumer must surface an
    inversion window for the high-priority message that waits behind it."""

    def test_batch_blocking_inversion_window(self):
        config = make_config(capacity=2, horizon=12)
        batches = [
            Batch(batch_id="b-low", priority=1, size=2, duration_rounds=5,
                  arrival_round=0),
        ]
        messages = [
            Message(message_id="hi-1", business_key="acct-9", priority=10,
                    arrival_round=1, processing_rounds=1,
                    visibility_timeout=4, deadline=8),
        ]
        report = PriorityInversionDetector(config).run(messages, batches)

        self.assertTrue(report.inversion_detected)
        self.assertEqual(len(report.inversion_windows), 1)

        window = report.inversion_windows[0]
        # Batch holds both consumers during rounds 0..4; hi-1 waits 1..4.
        self.assertEqual(window.start_round, 1)
        self.assertEqual(window.end_round, 4)
        self.assertEqual(window.duration_rounds, 4)
        self.assertEqual(window.message_id, "hi-1")
        self.assertEqual(window.blocking_consumers, [0, 1])
        self.assertEqual(window.blocking_items, ["batch:b-low"])

        # Both consumers were captured by lower-priority work.
        self.assertEqual(report.blocking_consumers, [0, 1])
        # Reserving one consumer would have absorbed the waiting burst.
        self.assertEqual(report.suggested_reserved_capacity, {10: 1})
        # The batch drained, so no starvation is reported.
        self.assertEqual(report.starvation_risks, [])
        # hi-1 finishes at round 6, within its deadline of 8.
        self.assertEqual(report.deadline_misses, [])
        self.assertEqual(report.completed_messages, 1)
        self.assertEqual(report.completed_batches, 1)

    def test_no_inversion_when_capacity_is_free(self):
        config = make_config(capacity=2, horizon=6)
        messages = [
            Message(message_id="hi-1", business_key="k1", priority=10,
                    arrival_round=0),
            Message(message_id="lo-1", business_key="k2", priority=1,
                    arrival_round=0),
        ]
        report = PriorityInversionDetector(config).run(messages)
        self.assertFalse(report.inversion_detected)
        self.assertEqual(report.inversion_windows, [])
        self.assertEqual(report.completed_messages, 2)


class LeaseExpiryTest(unittest.TestCase):
    """Visibility-timeout expiry creates retries that must not bypass the
    original business-key order."""

    def test_lease_expiry_retries_and_preserves_key_order(self):
        config = make_config(capacity=1, horizon=6)
        messages = [
            # processing (3 rounds) outlives the lease (1 round) -> retries.
            Message(message_id="m1", business_key="order-1", priority=5,
                    arrival_round=0, processing_rounds=3,
                    visibility_timeout=1),
            Message(message_id="m2", business_key="order-1", priority=5,
                    arrival_round=0, processing_rounds=1,
                    visibility_timeout=10),
        ]
        report = PriorityInversionDetector(config).run(messages)

        # m1's lease expires at rounds 1..5 and it is retried each time.
        self.assertGreaterEqual(report.total_retries, 3)
        # m1 never completes, so m2 must never start: key order is kept and
        # the retry never lets m2 jump ahead of m1.
        self.assertEqual(report.completed_messages, 0)
        # Same-priority waiting behind its own key is not an inversion.
        self.assertFalse(report.inversion_detected)

    def test_message_completes_when_lease_covers_processing(self):
        config = make_config(capacity=1, horizon=6)
        messages = [
            Message(message_id="m1", business_key="order-1", priority=5,
                    arrival_round=0, processing_rounds=2,
                    visibility_timeout=5),
            Message(message_id="m2", business_key="order-1", priority=5,
                    arrival_round=0, processing_rounds=1,
                    visibility_timeout=5),
        ]
        report = PriorityInversionDetector(config).run(messages)
        self.assertEqual(report.total_retries, 0)
        self.assertEqual(report.completed_messages, 2)

    def test_lease_expiry_repreemption_causes_inversion(self):
        # A low-priority message whose lease keeps expiring re-occupies the
        # only consumer, while a high-priority message waits: inversion.
        config = make_config(capacity=1, horizon=6)
        messages = [
            Message(message_id="lo-flaky", business_key="k-lo", priority=1,
                    arrival_round=0, processing_rounds=3,
                    visibility_timeout=1),
            # Enough high-priority demand that the flaky low-priority retry
            # wins a scheduling round on fair-share score and re-preempts.
            Message(message_id="hi-1", business_key="k-hi-1", priority=10,
                    arrival_round=1, processing_rounds=1,
                    visibility_timeout=10),
            Message(message_id="hi-2", business_key="k-hi-2", priority=10,
                    arrival_round=1, processing_rounds=1,
                    visibility_timeout=10),
            Message(message_id="hi-3", business_key="k-hi-3", priority=10,
                    arrival_round=1, processing_rounds=1,
                    visibility_timeout=10),
        ]
        report = PriorityInversionDetector(config).run(messages)
        self.assertTrue(report.inversion_detected)
        hi_windows = [w for w in report.inversion_windows
                      if w.message_id.startswith("hi-")]
        self.assertTrue(hi_windows)
        self.assertIn("message:lo-flaky", hi_windows[0].blocking_items)
        self.assertGreaterEqual(report.total_retries, 1)


class FairShareTest(unittest.TestCase):
    """Weighted-fair scheduling splits capacity proportionally; extreme
    weights are flagged as a starvation risk instead of silently starving."""

    @staticmethod
    def _saturated_messages(config, prio_lo=1, prio_hi=5):
        # Demand per priority exceeds total capacity so the fairness
        # weights (not queue drain) decide the split.
        count_per_prio = config.consumer_capacity * config.horizon_rounds
        messages = []
        for i in range(count_per_prio):
            messages.append(Message(
                message_id="lo-%d" % i, business_key="lo-key-%d" % i,
                priority=prio_lo, arrival_round=0, visibility_timeout=10))
            messages.append(Message(
                message_id="hi-%d" % i, business_key="hi-key-%d" % i,
                priority=prio_hi, arrival_round=0, visibility_timeout=10))
        return messages

    def test_weighted_fair_share(self):
        config = make_config(capacity=2, weights={1: 1.0, 5: 3.0}, horizon=42)
        messages = self._saturated_messages(config)
        report = PriorityInversionDetector(config).run(messages)

        served = report.served_units_by_priority
        self.assertGreater(served[1], 0)
        ratio = served[5] / float(served[1])
        # Ideal weighted share is 3:1.
        self.assertGreater(ratio, 2.0)
        self.assertLess(ratio, 4.5)
        # Both levels are served proportionally: nobody starves.
        self.assertEqual(report.starvation_risks, [])

    def test_extreme_weight_starves_low_priority(self):
        config = make_config(capacity=2, weights={1: 1.0, 9: 1000.0},
                             horizon=60)
        messages = self._saturated_messages(config, prio_lo=1, prio_hi=9)
        report = PriorityInversionDetector(config).run(messages)

        risks = {r.priority: r for r in report.starvation_risks}
        self.assertIn(1, risks)
        risk = risks[1]
        self.assertEqual(risk.level, "high")
        self.assertLessEqual(risk.served_units, 2)
        # Service rate collapses even though demand persists all horizon.
        self.assertEqual(risk.demand_rounds, 60)
        # The dominant priority is not flagged.
        self.assertNotIn(9, risks)


class DuplicateMessageTest(unittest.TestCase):
    """Duplicate submissions of the same dedup key are processed once."""

    def test_duplicate_dedup_key_is_dropped(self):
        config = make_config(capacity=2, horizon=8)
        messages = [
            Message(message_id="m1", business_key="k1", priority=5,
                    arrival_round=0, dedup_key="order-42"),
            Message(message_id="m1-copy", business_key="k2", priority=5,
                    arrival_round=1, dedup_key="order-42"),
            Message(message_id="m2", business_key="k3", priority=5,
                    arrival_round=1, dedup_key="order-43"),
        ]
        report = PriorityInversionDetector(config).run(messages)
        self.assertEqual(report.duplicates_dropped, 1)
        self.assertEqual(report.completed_messages, 2)

    def test_duplicate_without_dedup_key_falls_back_to_message_id(self):
        config = make_config(capacity=1, horizon=8)
        messages = [
            Message(message_id="m1", business_key="k1", priority=5,
                    arrival_round=0),
            Message(message_id="m1", business_key="k1", priority=5,
                    arrival_round=1),
        ]
        report = PriorityInversionDetector(config).run(messages)
        self.assertEqual(report.duplicates_dropped, 1)
        self.assertEqual(report.completed_messages, 1)


if __name__ == "__main__":
    unittest.main()
