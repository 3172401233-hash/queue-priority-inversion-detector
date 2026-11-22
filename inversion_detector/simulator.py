"""Round-based scheduling simulator and priority-inversion detector.

Scheduling policy per round
---------------------------
1. Finish batches / messages whose work completes this round.
2. Expire leases: an in-flight message whose ``visibility_timeout`` elapses
   before its work finishes returns to the *front* of its business-key queue
   as a retry -- retries never bypass the original per-key order.
3. Admit arrivals (duplicate ``dedup_key`` submissions are dropped).
4. Assign free consumers with weighted-fair scheduling across priority
   levels (lowest ``served / weight`` ratio first, higher priority wins
   ties).  Only the head message of each business key is eligible.
5. Detect inversion: any eligible-but-unserved message whose priority is
   higher than some currently running item records an inversion round.

The result is a purely offline scheduling plan; nothing is dispatched to a
real broker.
"""

from collections import defaultdict, deque

from .models import SchedulerConfig
from .report import (
    DeadlineMiss,
    DetectionReport,
    InversionWindow,
    StarvationRisk,
)


class _InFlight(object):
    __slots__ = ("message", "consumer_id", "finish_round", "lease_expiry_round")

    def __init__(self, message, consumer_id, finish_round, lease_expiry_round):
        self.message = message
        self.consumer_id = consumer_id
        self.finish_round = finish_round
        self.lease_expiry_round = lease_expiry_round


class _RunningBatch(object):
    __slots__ = ("batch", "consumer_ids", "end_round")

    def __init__(self, batch, consumer_ids, end_round):
        self.batch = batch
        self.consumer_ids = consumer_ids
        self.end_round = end_round


class _RoundInversion(object):
    __slots__ = ("round", "message", "blocking_consumers", "blocking_items")

    def __init__(self, round_no, message, blocking_consumers, blocking_items):
        self.round = round_no
        self.message = message
        self.blocking_consumers = blocking_consumers
        self.blocking_items = blocking_items


class PriorityInversionDetector(object):
    """Simulates scheduling rounds and detects priority inversion."""

    STARVATION_DEMAND_FRACTION = 0.2
    STARVATION_DEFICIT_THRESHOLD = 0.5
    STARVATION_HIGH_THRESHOLD = 0.75
    STARVATION_RATE_FLOOR = 0.05
    STARVATION_RATE_HIGH = 0.02

    def __init__(self, config):
        # type: (SchedulerConfig) -> None
        if config.consumer_capacity < 1:
            raise ValueError("consumer_capacity must be >= 1")
        if config.horizon_rounds < 1:
            raise ValueError("horizon_rounds must be >= 1")
        self.config = config

    def run(self, messages, batches=None):
        """Simulate and return a :class:`DetectionReport`."""
        cfg = self.config
        capacity = cfg.consumer_capacity
        horizon = cfg.horizon_rounds

        arrivals = defaultdict(list)
        batch_arrivals = defaultdict(list)
        for msg in messages:
            if msg.arrival_round < horizon:
                arrivals[msg.arrival_round].append(msg)
        for batch in batches or []:
            if batch.arrival_round < horizon:
                batch_arrivals[batch.arrival_round].append(batch)

        key_queues = defaultdict(deque)  # business_key -> FIFO of Message
        key_busy = set()  # business keys with an in-flight message
        seen_dedup = set()
        pending_batches = []
        in_flight = {}  # message_id -> _InFlight
        running_batches = {}  # batch_id -> _RunningBatch
        free_consumers = list(range(capacity))

        served_units = defaultdict(int)
        demand_rounds = defaultdict(int)
        demand_units = defaultdict(int)
        waiting_counts = defaultdict(lambda: defaultdict(int))
        inversion_records = []
        deadline_misses = []
        completed_messages = 0
        completed_batches = 0
        duplicates_dropped = 0

        for t in range(horizon):
            # 1. completions -------------------------------------------------
            for bid in [b for b, rb in running_batches.items() if rb.end_round <= t]:
                rb = running_batches.pop(bid)
                free_consumers.extend(rb.consumer_ids)
                completed_batches += 1
                if rb.batch.deadline is not None and rb.end_round > rb.batch.deadline:
                    deadline_misses.append(
                        DeadlineMiss(bid, "batch", rb.batch.priority,
                                     rb.batch.deadline, rb.end_round))
            for mid in [m for m, inf in in_flight.items() if inf.finish_round <= t]:
                inf = in_flight.pop(mid)
                free_consumers.append(inf.consumer_id)
                key_busy.discard(inf.message.business_key)
                completed_messages += 1
                if (inf.message.deadline is not None
                        and inf.finish_round > inf.message.deadline):
                    deadline_misses.append(
                        DeadlineMiss(mid, "message", inf.message.priority,
                                     inf.message.deadline, inf.finish_round))

            # 2. lease expiry -> retry at the front of the same key ----------
            for mid in [m for m, inf in in_flight.items()
                        if inf.lease_expiry_round <= t]:
                inf = in_flight.pop(mid)
                free_consumers.append(inf.consumer_id)
                msg = inf.message
                msg.retry_count += 1
                key_busy.discard(msg.business_key)
                key_queues[msg.business_key].appendleft(msg)

            # 3. arrivals (deduplicated) -------------------------------------
            for msg in arrivals.get(t, ()):
                dedup_key = msg.dedup_key or msg.message_id
                if dedup_key in seen_dedup:
                    duplicates_dropped += 1
                    continue
                seen_dedup.add(dedup_key)
                key_queues[msg.business_key].append(msg)
            pending_batches.extend(batch_arrivals.get(t, ()))

            # 4. weighted-fair scheduling ------------------------------------
            while free_consumers:
                head_msgs = [
                    q[0] for key, q in key_queues.items()
                    if q and key not in key_busy
                ]
                schedulable_batches = [
                    b for b in pending_batches if b.size <= len(free_consumers)
                ]
                if not head_msgs and not schedulable_batches:
                    break
                priorities = {m.priority for m in head_msgs}
                priorities.update(b.priority for b in schedulable_batches)
                chosen = min(
                    priorities,
                    key=lambda p: (served_units[p] / cfg.weight_for(p), -p),
                )
                msgs_at_prio = [m for m in head_msgs if m.priority == chosen]
                if msgs_at_prio:
                    msgs_at_prio.sort(
                        key=lambda m: (m.arrival_round, m.message_id))
                    msg = msgs_at_prio[0]
                    consumer = free_consumers.pop()
                    key_queues[msg.business_key].popleft()
                    key_busy.add(msg.business_key)
                    in_flight[msg.message_id] = _InFlight(
                        msg, consumer,
                        t + msg.processing_rounds,
                        t + msg.visibility_timeout,
                    )
                    served_units[chosen] += 1
                    continue
                batches_at_prio = [
                    b for b in schedulable_batches if b.priority == chosen
                ]
                if batches_at_prio:
                    batches_at_prio.sort(key=lambda b: b.arrival_round)
                    batch = batches_at_prio[0]
                    pending_batches.remove(batch)
                    consumers = [free_consumers.pop() for _ in range(batch.size)]
                    running_batches[batch.batch_id] = _RunningBatch(
                        batch, consumers, t + batch.duration_rounds)
                    served_units[chosen] += batch.size
                    continue
                break  # chosen priority only has unschedulable batches

            # 5. inversion detection + demand tracking -----------------------
            running_items = []
            for inf in in_flight.values():
                running_items.append((
                    inf.message.priority,
                    inf.consumer_id,
                    "message:" + inf.message.message_id,
                ))
            for rb in running_batches.values():
                for consumer in rb.consumer_ids:
                    running_items.append((
                        rb.batch.priority,
                        consumer,
                        "batch:" + rb.batch.batch_id,
                    ))

            for key, q in key_queues.items():
                if not q or key in key_busy:
                    continue
                msg = q[0]
                blockers = [item for item in running_items
                            if item[0] < msg.priority]
                if blockers:
                    inversion_records.append(_RoundInversion(
                        t, msg,
                        sorted({item[1] for item in blockers}),
                        sorted({item[2] for item in blockers}),
                    ))
                    waiting_counts[t][msg.priority] += 1

            demand_priorities = {
                q[0].priority
                for key, q in key_queues.items()
                if q and key not in key_busy
            }
            demand_priorities.update(b.priority for b in pending_batches)
            for prio in demand_priorities:
                demand_rounds[prio] += 1
            for key, q in key_queues.items():
                if q and key not in key_busy:
                    demand_units[q[0].priority] += 1
            for batch in pending_batches:
                demand_units[batch.priority] += batch.size

        # ---- horizon end: unfinished work with a deadline ------------------
        finished_ids = {m.item_id for m in deadline_misses}
        for key, q in key_queues.items():
            for msg in q:
                if (msg.deadline is not None and msg.deadline < horizon
                        and msg.message_id not in finished_ids):
                    deadline_misses.append(
                        DeadlineMiss(msg.message_id, "message", msg.priority,
                                     msg.deadline, None))
        for inf in in_flight.values():
            msg = inf.message
            if (msg.deadline is not None and msg.deadline < horizon
                    and msg.message_id not in finished_ids):
                deadline_misses.append(
                    DeadlineMiss(msg.message_id, "message", msg.priority,
                                 msg.deadline, None))
        for batch in pending_batches:
            if batch.deadline is not None and batch.deadline < horizon:
                deadline_misses.append(
                    DeadlineMiss(batch.batch_id, "batch", batch.priority,
                                 batch.deadline, None))

        residual_units = defaultdict(int)
        for key, q in key_queues.items():
            for msg in q:
                residual_units[msg.priority] += 1
        for batch in pending_batches:
            residual_units[batch.priority] += batch.size

        return self._build_report(
            inversion_records, waiting_counts, served_units, demand_rounds,
            demand_units, residual_units, deadline_misses, duplicates_dropped,
            completed_messages, completed_batches, messages,
        )

    # ------------------------------------------------------------------ #
    def _build_report(self, inversion_records, waiting_counts, served_units,
                      demand_rounds, demand_units, residual_units,
                      deadline_misses, duplicates_dropped, completed_messages,
                      completed_batches, messages):
        cfg = self.config
        capacity = cfg.consumer_capacity
        horizon = cfg.horizon_rounds

        windows = self._group_windows(inversion_records)
        blocking_consumers = sorted({
            consumer for w in windows for consumer in w.blocking_consumers
        })

        # Reserved capacity: enough consumers to serve the worst observed
        # burst of simultaneously waiting messages at each inverted priority.
        suggested = {}
        for t, per_prio in waiting_counts.items():
            for prio, count in per_prio.items():
                suggested[prio] = max(suggested.get(prio, 0), count)
        suggested = {p: min(capacity, c) for p, c in suggested.items()}

        starvation = self._starvation_risks(
            served_units, demand_rounds, demand_units, residual_units)

        return DetectionReport(
            horizon_rounds=horizon,
            consumer_capacity=capacity,
            inversion_windows=windows,
            blocking_consumers=blocking_consumers,
            suggested_reserved_capacity=suggested,
            starvation_risks=starvation,
            deadline_misses=deadline_misses,
            duplicates_dropped=duplicates_dropped,
            total_retries=sum(m.retry_count for m in messages),
            completed_messages=completed_messages,
            completed_batches=completed_batches,
            served_units_by_priority=dict(served_units),
        )

    @staticmethod
    def _group_windows(records):
        by_message = defaultdict(list)
        for rec in records:
            by_message[rec.message.message_id].append(rec)
        windows = []
        for mid, recs in by_message.items():
            recs.sort(key=lambda r: r.round)
            group = [recs[0]]
            for rec in recs[1:]:
                if rec.round == group[-1].round + 1:
                    group.append(rec)
                else:
                    windows.append(PriorityInversionDetector._window_from(group))
                    group = [rec]
            windows.append(PriorityInversionDetector._window_from(group))
        windows.sort(key=lambda w: (w.start_round, -w.priority, w.message_id))
        return windows

    @staticmethod
    def _window_from(group):
        first = group[0]
        return InversionWindow(
            message_id=first.message.message_id,
            business_key=first.message.business_key,
            priority=first.message.priority,
            start_round=group[0].round,
            end_round=group[-1].round,
            blocking_consumers=sorted({
                c for rec in group for c in rec.blocking_consumers}),
            blocking_items=sorted({
                item for rec in group for item in rec.blocking_items}),
        )

    def _starvation_risks(self, served_units, demand_rounds, demand_units,
                          residual_units):
        cfg = self.config
        horizon = cfg.horizon_rounds
        total_slots = cfg.consumer_capacity * horizon
        weight_sum = sum(
            cfg.weight_for(p) for p in demand_rounds
        ) or 1.0
        risks = []
        min_demand = max(1, int(horizon * self.STARVATION_DEMAND_FRACTION))
        for prio in sorted(demand_rounds):
            if demand_rounds[prio] < min_demand:
                continue
            if residual_units.get(prio, 0) <= 0:
                continue  # demand was fully drained: blocked, not starved
            proportional = total_slots * cfg.weight_for(prio) / weight_sum
            fair_share = min(float(demand_units[prio]), proportional)
            if fair_share <= 0:
                continue
            served = served_units.get(prio, 0)
            deficit = max(0.0, min(1.0, 1.0 - served / fair_share))
            rate = served / float(demand_rounds[prio])
            if (deficit >= self.STARVATION_DEFICIT_THRESHOLD
                    or rate < self.STARVATION_RATE_FLOOR):
                level = ("high"
                         if (deficit >= self.STARVATION_HIGH_THRESHOLD
                             or rate < self.STARVATION_RATE_HIGH)
                         else "medium")
                risks.append(StarvationRisk(
                    priority=prio,
                    demand_rounds=demand_rounds[prio],
                    served_units=served,
                    fair_share_units=fair_share,
                    deficit_ratio=deficit,
                    level=level,
                ))
        return risks
