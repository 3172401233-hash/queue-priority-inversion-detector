"""Queue priority-inversion detector (offline scheduling plan generator).

Simulates round-based scheduling of prioritized messages and batches against
a fixed consumer capacity, then reports priority-inversion windows, blocking
consumers, suggested reserved capacity and starvation risks.

The package only produces an *offline* scheduling plan / analysis report.
It never connects to a live message broker.
"""

from .models import Batch, Message, SchedulerConfig
from .report import DetectionReport, InversionWindow, StarvationRisk
from .simulator import PriorityInversionDetector

__all__ = [
    "Batch",
    "DetectionReport",
    "InversionWindow",
    "Message",
    "PriorityInversionDetector",
    "SchedulerConfig",
    "StarvationRisk",
]

__version__ = "0.1.0"
