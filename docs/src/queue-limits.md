# The write-behind queue's limits

The ui-tape's writer is a bounded-queue design. Two bounded queues
carry the two streams a deployment produces, and each states its own law
at its boundary. This page states the limits and what crossing them
means.

## The two queues

| Queue | Carries | Capacity | Boundary law |
|---|---|---|---|
| The AOF event queue | `LKE1` lock-event records, the durable stream | 4,096 events (`AofConfig.queue_cap`) | an enqueue onto a full queue crashes the process |
| The membership sidecar | membership snapshots, observability | 64 snapshots (`membership::SidecarWriter`) | an enqueue onto a full queue drops the snapshot and counts it |

The AOF queue is the durable stream's queue: a record on it is a lease
event the UI server's series must hold, and losing one silently is the
most expensive bug a capture can have. The sidecar's queue carries a
snapshot that the next snapshot supersedes; dropping one under pressure
loses nothing that matters, and the drop counter names the pressure. The
boundary laws differ because the streams differ.

## The AOF queue's numbers

- **Record size**: 61 bytes, fixed — every `LKE1` record is the same
  shape. The queue's worst case is 4,096 × 61 B ≈ **250 KB** of records.
- **The drain cadence**: the writer thread appends each record into a
  64 KiB buffer and flushes the buffer at 200 ms ticks or at 64 KiB,
  whichever first — roughly 1,075 records per flush. An append to the
  buffer costs microseconds; the queue's depth in ordinary operation is
  about one.
- **The durability cadence**: the file is fsync'd on the 1 s timer (the
  operator knob), on an explicit checkpoint, and on the drain that the
  clean shutdown takes.
- **The roll**: the active file rolls at exactly 2 MiB — about 34,400
  records per file.

## What a full queue means

The queue fills only when the writer thread's appends stop moving: the
buffer flush's `write_all` or the fsync is blocked on the disk. A full
queue is therefore not a traffic problem — it is a **disk stall**, and
the candidates are a full disk, a hung network filesystem, or a wedged
block device.

Because the producers never block, the queue converts the disk stall
into bounded memory: the capture absorbs up to 4,096 events of stall and
then crashes the process. At the demo rig's measured steady state —
about **100 events per second** arriving at a ui-tape writer that fronts a
three-member voting fleet — the queue holds about **41 seconds** of
total disk stall before the boundary. Operators who expect slower disks
raise `queue_cap`: the stall tolerance grows linearly with the cap, and
the worst-case memory grows with it.

The crash is the point. A node whose disk is full cannot complete a
clean shutdown, cannot flush its frame, and cannot write its markers —
the stall that filled the queue is the same stall that would break the
shutdown ceremony. Aborting at the queue's boundary surfaces the disk
first, while the operator can still act: a fleet of identical machines
fills its disks in the same order under the same load, and the first
abort names the condition before the second machine reaches it. A
capture that stayed up through the stall would be the outage's quiet
accomplice.

## The sizing inheritance

The capture's shape inherits the fixed-allocation discipline of the
tbio-core lineage: the record is a fixed 61 bytes, the file is a fixed
2 MiB erasure block, and the queue is a fixed event count. What is
bounded is the whole pipeline — the queue's count bounds the backlog,
the byte counter bounds the flush, and the erasure block bounds the
file. Nothing between a producer's enqueue and the fsync is unbounded.

The membership sidecar is the one deliberate exception: its snapshots
are a running supersession, its loss is priced at zero, and its
boundary law is drop-and-count.
