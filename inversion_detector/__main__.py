"""Demo entry point: ``python -m inversion_detector``.

Builds a small scenario (a low-priority batch occupying the whole consumer
pool while a high-priority message waits) and prints the offline scheduling
plan as JSON.  No live broker is involved.
"""

import json

from .models import Batch, Message, SchedulerConfig
from .simulator import PriorityInversionDetector


def demo_report():
    config = SchedulerConfig(
        consumer_capacity=2,
        fairness_weights={1: 1.0, 10: 4.0},
        horizon_rounds=12,
    )
    batches = [
        Batch(batch_id="batch-low", priority=1, size=2, duration_rounds=6,
              arrival_round=0),
    ]
    messages = [
        Message(message_id="hi-1", business_key="acct-9", priority=10,
                arrival_round=1, processing_rounds=1, visibility_timeout=4,
                deadline=8),
        Message(message_id="lo-1", business_key="acct-1", priority=1,
                arrival_round=2, processing_rounds=1, visibility_timeout=4),
    ]
    detector = PriorityInversionDetector(config)
    return detector.run(messages, batches)


def main():
    report = demo_report()
    print(json.dumps(report.to_dict(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
