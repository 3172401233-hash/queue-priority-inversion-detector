"""Output data model: inversion windows, starvation risks, full report."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class InversionWindow:
    """Consecutive rounds during which a ready higher-priority message waited
    while lower-priority work occupied consumers."""

    message_id: str
    business_key: str
    priority: int
    start_round: int
    end_round: int  # inclusive
    blocking_consumers: List[int] = field(default_factory=list)
    blocking_items: List[str] = field(default_factory=list)

    @property
    def duration_rounds(self):
        # type: () -> int
        return self.end_round - self.start_round + 1

    def to_dict(self):
        return {
            "message_id": self.message_id,
            "business_key": self.business_key,
            "priority": self.priority,
            "start_round": self.start_round,
            "end_round": self.end_round,
            "duration_rounds": self.duration_rounds,
            "blocking_consumers": list(self.blocking_consumers),
            "blocking_items": list(self.blocking_items),
        }


@dataclass
class StarvationRisk:
    """A priority level whose observed service share falls far below its
    configured fair share."""

    priority: int
    demand_rounds: int
    served_units: int
    fair_share_units: float
    deficit_ratio: float  # 1 - served / fair_share, in [0, 1]
    level: str  # "medium" | "high"

    def to_dict(self):
        return {
            "priority": self.priority,
            "demand_rounds": self.demand_rounds,
            "served_units": self.served_units,
            "fair_share_units": round(self.fair_share_units, 2),
            "deficit_ratio": round(self.deficit_ratio, 3),
            "level": self.level,
        }


@dataclass
class DeadlineMiss:
    item_id: str
    kind: str  # "message" | "batch"
    priority: int
    deadline: int
    finished_round: Optional[int]  # None means still unfinished at horizon end

    def to_dict(self):
        return {
            "item_id": self.item_id,
            "kind": self.kind,
            "priority": self.priority,
            "deadline": self.deadline,
            "finished_round": self.finished_round,
        }


@dataclass
class DetectionReport:
    """Full offline scheduling plan / inversion analysis."""

    horizon_rounds: int
    consumer_capacity: int
    inversion_windows: List[InversionWindow] = field(default_factory=list)
    blocking_consumers: List[int] = field(default_factory=list)
    suggested_reserved_capacity: Dict[int, int] = field(default_factory=dict)
    starvation_risks: List[StarvationRisk] = field(default_factory=list)
    deadline_misses: List[DeadlineMiss] = field(default_factory=list)
    duplicates_dropped: int = 0
    total_retries: int = 0
    completed_messages: int = 0
    completed_batches: int = 0
    served_units_by_priority: Dict[int, int] = field(default_factory=dict)

    @property
    def inversion_detected(self):
        # type: () -> bool
        return bool(self.inversion_windows)

    def to_dict(self):
        return {
            "plan_kind": "offline_scheduling_plan",
            "horizon_rounds": self.horizon_rounds,
            "consumer_capacity": self.consumer_capacity,
            "inversion_detected": self.inversion_detected,
            "inversion_windows": [w.to_dict() for w in self.inversion_windows],
            "blocking_consumers": list(self.blocking_consumers),
            "suggested_reserved_capacity": {
                str(p): c for p, c in sorted(self.suggested_reserved_capacity.items())
            },
            "starvation_risks": [r.to_dict() for r in self.starvation_risks],
            "deadline_misses": [m.to_dict() for m in self.deadline_misses],
            "duplicates_dropped": self.duplicates_dropped,
            "total_retries": self.total_retries,
            "completed_messages": self.completed_messages,
            "completed_batches": self.completed_batches,
            "served_units_by_priority": {
                str(p): u for p, u in sorted(self.served_units_by_priority.items())
            },
        }
