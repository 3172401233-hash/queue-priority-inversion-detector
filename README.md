# queue-priority-inversion-detector
检测消息队列优先级反转、租约和重试抢占

> **重要：本工具只生成离线调度计划（offline scheduling plan）。**
> 它根据输入的消息、批次、容量与权重在本地模拟每一轮调度并输出分析报告，
> 不连接、不消费、不投递任何真实的消息队列 / broker。

## 问题背景

高优先级消息因为低优先级批次占用全部消费者而等待，低优先级消息又在
visibility timeout 后重新抢占消费者，形成**优先级反转**。而单纯提高高优先级
权重又会让低优先级永久饥饿。本检测器按轮模拟调度过程，量化这一权衡。

## 调度模型

每轮依次执行：

1. 完成到期的批次 / 消息，释放消费者；
2. 租约（visibility timeout）过期的在飞消息回到**原业务键队首**重试——
   重试不能绕过原键顺序；
3. 新消息入队（相同 `dedup_key` 的重复消息被丢弃）；
4. 按加权公平调度（`served / weight` 最小者优先，平局高优先级优先）分配空闲
   消费者；同一业务键只有队首消息可被调度，保证键内顺序；
5. 检测反转：存在就绪但未调度的高优先级消息，且某消费者正在运行更低优先级
   的消息或批次，则记为一个反转轮。

## 输入

- `Message`：`message_id`、`business_key`、`priority`（越大越紧急）、
  `arrival_round`、`processing_rounds`、`visibility_timeout`、`deadline`、
  `dedup_key`；
- `Batch`：`batch_id`、`priority`、`size`（原子占用的消费者数）、
  `duration_rounds`、`arrival_round`、`deadline`；
- `SchedulerConfig`：`consumer_capacity`、`fairness_weights`
  （优先级 → 公平权重）、`horizon_rounds`。

## 输出（`DetectionReport`）

- `inversion_windows`：反转时间窗（起止轮、等待消息、阻塞消费者、阻塞项）；
- `blocking_consumers`：参与阻塞的消费者列表；
- `suggested_reserved_capacity`：建议为高优先级预留的消费者容量
  （按反转窗口内同时等待消息的最大突发量计算）；
- `starvation_risks`：饥饿风险（持续有需求、周期结束仍有积压，且公平份额
  赤字或服务率低于下限的优先级，分 medium / high 两级）；
- `deadline_misses`、`duplicates_dropped`、`total_retries` 等统计。

## 使用

```python
from inversion_detector import Batch, Message, PriorityInversionDetector, SchedulerConfig

config = SchedulerConfig(consumer_capacity=2,
                         fairness_weights={1: 1.0, 10: 4.0},
                         horizon_rounds=12)
report = PriorityInversionDetector(config).run(
    messages=[Message("hi-1", "acct-9", priority=10, arrival_round=1,
                      visibility_timeout=4, deadline=8)],
    batches=[Batch("b-low", priority=1, size=2, duration_rounds=5)],
)
print(report.to_dict())  # 离线调度计划 + 反转分析
```

运行内置示例（打印 JSON 格式的离线调度计划）：

```bash
python -m inversion_detector
```

## 测试

兼容 Python 3.8.2+，仅使用标准库：

```bash
python -m unittest
```

测试覆盖：批次阻塞导致的反转时间窗、租约过期重试与键内顺序、
加权公平份额与极端权重下的饥饿风险、重复消息去重。
