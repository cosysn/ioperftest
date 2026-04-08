# IOperf Async IO Design

## Overview

Port ioperf module to pure async, lock-free architecture similar to SPDK. Workers submit IO and return immediately without waiting. Poller threads busy-poll on dedicated CPU cores with no sleep.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│ Worker Thread (runs on any core)                        │
│   └─ ioperf_submit_io() → returns immediately           │
│                              │                           │
│                              ▼                           │
│                    channel->wait_queue (lock-free)     │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│ Poller Thread (bound to dedicated cores via affinity)  │
│   └─ while(poller_running) { busy-poll wait_queue }   │
│                              │                           │
│                              │ latency expired           │
│                              ▼                           │
│                    ioperf_process_io()                 │
│                              │                           │
│                              ▼                           │
│               callback(io_complete) → update stats      │
└─────────────────────────────────────────────────────────┘
```

## Data Structures

### ioperf_opts (public API)
```c
struct ioperf_opts {
    char     *name;
    uint64_t  num_blocks;
    uint32_t  block_size;
    uint32_t  physical_block_size;
    uint32_t  num_threads;
    uint64_t  read_latency_us;
    uint64_t  write_latency_us;
    bool      enable_validation;
    uint32_t *poller_cpus;       /* NEW: CPU IDs for poller affinity */
    uint32_t  poller_cpus_count; /* NEW: length of poller_cpus array */
};
```

### ioperf_disk (internal)
```c
struct ioperf_disk {
    // ... existing fields ...
    uint32_t *poller_cpus;       /* NEW */
    uint32_t  poller_cpus_count; /* NEW */
};
```

## Implementation Details

### 1. Poller CPU Affinity

In `ioperf_disk_create()`, when creating poller threads:
```c
if (disk->poller_cpus_count > 0 && i < disk->poller_cpus_count) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(disk->poller_cpus[i], &cpuset);
    pthread_setaffinity_np(ch->poller_thread, sizeof(cpu_set_t), &cpuset);
}
```

### 2. Busy-Poll Poller (No Sleep)

```c
static void *
ioperf_wait_poll_thread(void *arg)
{
    struct ioperf_channel *ch = (struct ioperf_channel *)arg;
    struct ioperf_disk *disk = ch->disk;

    while (ch->poller_running) {
        uint64_t now = ioperf_get_ticks();

        /* Process wait queue - always busy poll, no sleep */
        struct ioperf_io_ctx **pprev = &ch->wait_queue_head;
        struct ioperf_io_ctx *io_ctx = ch->wait_queue_head;
        while (io_ctx) {
            uint64_t delay = (io_ctx->type == IOPERF_IO_READ) ?
                disk->read_latency_ns : disk->write_latency_ns;
            if (now - io_ctx->queued_ticks >= delay) {
                *pprev = io_ctx->next;
                if (ch->wait_queue_tail == io_ctx) {
                    ch->wait_queue_tail = *pprev;
                }
                ch->wait_queue_count--;
                ioperf_process_io(io_ctx);
                io_ctx = *pprev;
            } else {
                pprev = &io_ctx->next;
                io_ctx = io_ctx->next;
            }
        }

        /* Process rate limit queue */
        /* ... rate_limit_check logic ... */
    }
    return NULL;
}
```

### 3. Async Worker (Benchmark)

Worker submits IO and continues immediately without waiting:
```c
static void *
seq_read_worker(void *arg)
{
    struct thread_stats *stats = (struct thread_stats *)arg;
    struct io_wrapper *ios;
    uint32_t idx = 0;
    uint32_t io_depth = DEFAULT_IO_DEPTH;

    ios = calloc(io_depth, sizeof(struct io_wrapper));
    if (!ios) return NULL;

    while (g_running) {
        uint32_t batch_size = 0;
        while (batch_size < io_depth) {
            struct io_wrapper *w = &ios[idx % io_depth];

            ioperf_io_ctx_init(&w->ctx);
            w->ctx.type = IOPERF_IO_READ;
            w->ctx.offset_blocks = /* ... */;
            w->ctx.num_blocks = 1;
            w->ctx.io_size = g_disk->block_size;
            w->ctx.complete = io_complete;
            w->ctx.complete_arg = w;
            w->stats = stats;
            w->submit_ns = get_time_ns();

            /* Submit and return immediately - no spin wait */
            ioperf_submit_io(g_disk, &w->ctx);
            atomic_fetch_add(&g_tracker.in_flight, 1);

            batch_size++;
            idx++;
        }
        /* Continue submitting without waiting for completion */
    }
    /* ... cleanup ... */
}
```

### 4. Completion Callback

```c
static void
io_complete(void *arg)
{
    struct io_wrapper *w = (struct io_wrapper *)arg;
    uint64_t complete_ns = get_time_ns();
    uint64_t latency = complete_ns - w->submit_ns;

    w->stats->io_completed++;
    w->stats->bytes_completed += w->ctx.io_size;
    w->stats->total_latency_ns += latency;
    /* ... min/max latency tracking ... */

    uint32_t prev = atomic_fetch_sub(&g_tracker.in_flight, 1);
    /* ... signal if needed ... */
}
```

### 5. Benchmark CLI Changes

```bash
./ioperf_benchmark --read -t 4 -T 10 -c 0,2,4,6
# -c 0,2,4,6  指定 poller 线程绑定的 CPU 核心（逗号分隔）
```

New option in `benchmark_opts`:
```c
struct benchmark_opts {
    // ... existing ...
    uint32_t *poller_cpus;
    uint32_t  poller_cpus_count;
};
```

## Files to Modify

| File | Changes |
|------|---------|
| `lib/ioperf/ioperf.h` | Add `poller_cpus` fields to `ioperf_opts` and `ioperf_disk` |
| `lib/ioperf/ioperf.c` | Implement CPU affinity, busy-poll poller, rate limit queue |
| `tools/ioperf_benchmark.c` | Add `-c` CLI option, truly async workers |
| `Makefile` | No changes needed |

## Lock-Free Properties

1. **Wait queue** — Single producer (worker), single consumer (poller) → safe without lock
2. **Rate limit queue** — Single producer, single consumer → safe without lock
3. **Stats** — Per-thread stats structure → no contention between workers
4. **In-flight counter** — `_Atomic uint32_t` → lock-free atomic operations
5. **IO completion** — `_Atomic` flag not needed since callback is serialized by design

## Validation

- Benchmark runs without latency: expect ~15M IOPS
- Benchmark runs with 100us latency: poller processes IOs at correct rate
- CPU affinity: `ps -o cpu_list` shows poller threads on specified cores
- No spin-wait in worker threads
