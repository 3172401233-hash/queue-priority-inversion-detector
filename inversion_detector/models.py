"""Input data model for the priority-inversion detector.

Compatible with Python 3.8.2: only ``typing`` generics are used.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class Message:
    """A single queue message.

    ``priority`` is an integer where a *higher* value means more urgent.
    Messages sharing the same ``business_key`` must be processed in arrival
    order; a retry of a message never bypasses the original key order.
    """

    message_id: str
    business_key: str
    priority: int
    arrival_round: int = 0
    processing_rounds: int = 1
    visibility_timeout: int = 1
    deadline: Optional[int] = None
    dedup_key: Optional[str] = None
    retry_count: int = 0


@dataclass
class Batch:
    """An atomic batch that occupies ``size`` consumers for ``duration_rounds``.

    A batch starts only when at least ``size`` consumers are free; once
    started it holds all of them until ``duration_rounds`` elapse.  This is
    the classic source of priority inversion when a low-priority batch
    occupies the whole consumer pool.
    """

    batch_id: str
    priority: int
    size: int
    duration_rounds: int
    arrival_round: int = 0
    deadline: Optional[int] = None


@dataclass
class SchedulerConfig:
    """Simulation configuration.

    ``fairness_weights`` maps priority level to its weighted-fair-scheduling
    weight.  Raising the high-priority weight reduces inversion but can
    starve low priorities; the detector reports that trade-off instead of
    hiding it.
    """

    consumer_capacity: int
    fairness_weights: Dict[int, float] = field(default_factory=dict)
    horizon_rounds: int = 100

    def weight_for(self, priority):
        # type: (int) -> float
        return float(self.fairness_weights.get(priority, 1.0))
