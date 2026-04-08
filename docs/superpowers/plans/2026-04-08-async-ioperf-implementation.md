# IOperf Async IO Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement pure async, lock-free IO architecture - workers submit IO and return immediately, poller threads busy-poll on dedicated CPU cores with CPU affinity.

**Architecture:** Worker threads run on any core and submit IO to channel wait queues via ioperf_submit_io(). Poller threads are bound to dedicated CPU cores via pthread_setaffinity_np() and busy-poll their wait queues with no sleep, processing IOs when latency expires. Completion callbacks update stats asynchronously.

**Tech Stack:** C11 atomics, pthread affinity, lock-free SPSC queues

---

## File Structure

| File | Changes |
|------|---------|
| `lib/ioperf/ioperf.h` | Add `poller_cpus` fields to `ioperf_opts` and `ioperf_disk` |
| `lib/ioperf/ioperf.c` | Implement CPU affinity, busy-poll poller (remove nanosleep), rate limit queue |
| `tools/ioperf_benchmark.c` | Add `-c` CLI option, parse CPU list, update worker to truly async |

---

## Task 1: Modify ioperf.h - Add poller_cpus fields

**Files:**
- Modify: `lib/ioperf/ioperf.h:306-316`
- Modify: `lib/ioperf/ioperf.h:270-304`

- [ ] **Step 1: Add poller_cpus fields to ioperf_opts struct**

Read lines 306-316 of `lib/ioperf/ioperf.h`:
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
    // ADD THESE TWO FIELDS:
    uint32_t *poller_cpus;       /* CPU IDs for poller affinity, e.g. {0, 2, 4, 6} */
    uint32_t  poller_cpus_count; /* length of poller_cpus array */
};
```

- [ ] **Step 2: Add poller_cpus fields to ioperf_disk struct**

Read lines 270-304 of `lib/ioperf/ioperf.h`. Add after `num_threads` field:
```c
struct ioperf_disk {
    char                 name[32];
    uint32_t             block_size;
    uint32_t             physical_block_size;
    uint64_t             block_count;

    /* Configuration */
    uint32_t             num_threads;
    uint32_t            *poller_cpus;       /* NEW: CPU IDs for poller affinity */
    uint32_t             poller_cpus_count; /* NEW: length of poller_cpus array */
    uint64_t             read_latency_ns;
    uint64_t             write_latency_ns;
    // ... rest of fields unchanged ...
};
```

- [ ] **Step 3: Commit**

```bash
git add lib/ioperf/ioperf.h && git commit -m "feat: add poller_cpus fields to ioperf_opts and ioperf_disk"
```

---

## Task 2: Modify ioperf.c - Implement CPU Affinity and Busy-Poll Poller

**Files:**
- Modify: `lib/ioperf/ioperf.c:489-591` (ioperf_disk_create)
- Modify: `lib/ioperf/ioperf.c:346-411` (ioperf_wait_poll_thread)
- Modify: `lib/ioperf/ioperf.c:417-487` (ioperf_submit_io)

- [ ] **Step 1: Add includes for CPU affinity**

Read lines 1-10 of `lib/ioperf/ioperf.c`. Add sched.h include:
```c
#include "ioperf.h"
#include <stdio.h>
#include <errno.h>
#include <sched.h>  /* ADD: for CPU_SET and pthread_setaffinity_np */
```

- [ ] **Step 2: Modify ioperf_disk_create to set CPU affinity on poller threads**

Read lines 580-585 where poller thread is created:
```c
        disk->channels[i]->disk = disk;
        disk->channels[i]->thread_id = i;
        disk->channels[i]->poller_running = true;

        /* Start poller thread */
        pthread_create(&disk->channels[i]->poller_thread, NULL,
                      ioperf_wait_poll_thread, disk->channels[i]);
```

Replace with:
```c
        disk->channels[i]->disk = disk;
        disk->channels[i]->thread_id = i;
        disk->channels[i]->poller_running = true;

        /* Start poller thread */
        pthread_create(&disk->channels[i]->poller_thread, NULL,
                      ioperf_wait_poll_thread, disk->channels[i]);

        /* Set CPU affinity for poller thread if poller_cpus configured */
        if (disk->poller_cpus_count > 0 && i < disk->poller_cpus_count) {
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(disk->poller_cpus[i], &cpuset);
            pthread_setaffinity_np(disk->channels[i]->poller_thread,
                                   sizeof(cpu_set_t), &cpuset);
        }
```

- [ ] **Step 3: Copy poller_cpus from opts to disk in ioperf_disk_create**

Read lines 520-530 where opts are copied:
```c
    disk->read_latency_ns = opts->read_latency_us * 1000;
    disk->write_latency_ns = opts->write_latency_us * 1000;
    disk->enable_validation = opts->enable_validation;
```

Add after:
```c
    /* Copy poller CPU configuration */
    disk->poller_cpus_count = opts->poller_cpus_count;
    if (opts->poller_cpus_count > 0 && opts->poller_cpus != NULL) {
        disk->poller_cpus = calloc(opts->poller_cpus_count, sizeof(uint32_t));
        if (disk->poller_cpus == NULL) {
            ioperf_hash_map_destroy(&disk->hash_map_1);
            ioperf_hash_map_destroy(&disk->hash_map_2);
            free(disk);
            return -ENOMEM;
        }
        memcpy(disk->poller_cpus, opts->poller_cpus,
               opts->poller_cpus_count * sizeof(uint32_t));
    } else {
        disk->poller_cpus = NULL;
    }
```

- [ ] **Step 4: Free poller_cpus in ioperf_disk_destroy**

Read lines 593-617 of ioperf_disk_destroy. Add before hash_map_destroy:
```c
    /* Free poller_cpus array */
    if (disk->poller_cpus) {
        free(disk->poller_cpus);
        disk->poller_cpus = NULL;
    }
```

- [ ] **Step 5: Modify ioperf_wait_poll_thread - Remove nanosleep, busy-poll only**

Read lines 346-411. Replace the entire function with:
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
                /* Remove from queue */
                *pprev = io_ctx->next;
                if (ch->wait_queue_tail == io_ctx) {
                    ch->wait_queue_tail = *pprev;
                }
                ch->wait_queue_count--;

                /* Complete IO - this calls the callback */
                ioperf_process_io(io_ctx);

                /* Move to next (pprev stays same since we removed current) */
                io_ctx = *pprev;
            } else {
                /* Move to next */
                pprev = &io_ctx->next;
                io_ctx = io_ctx->next;
            }
        }

        /* Process rate limit queue */
        pprev = &ch->rate_limit_queue_head;
        io_ctx = ch->rate_limit_queue_head;
        while (io_ctx) {
            if (rate_limit_check(ch, disk, io_ctx->io_size)) {
                /* Remove from queue */
                *pprev = io_ctx->next;
                if (ch->rate_limit_queue_tail == io_ctx) {
                    ch->rate_limit_queue_tail = *pprev;
                }
                ch->rate_limit_queue_count--;

                /* Process IO */
                ioperf_process_io(io_ctx);

                /* Move to next (pprev stays same) */
                io_ctx = *pprev;
            } else {
                /* Move to next */
                pprev = &io_ctx->next;
                io_ctx = io_ctx->next;
            }
        }
        /* No nanosleep - busy poll continuously */
    }

    return NULL;
}
```

- [ ] **Step 6: Commit**

```bash
git add lib/ioperf/ioperf.c && git commit -m "feat: implement CPU affinity and busy-poll poller

- Set poller thread affinity to dedicated CPUs via pthread_setaffinity_np
- Remove nanosleep from poller - busy-poll wait_queue continuously
- Copy poller_cpus config from opts to disk in ioperf_disk_create
- Free poller_cpus array in ioperf_disk_destroy"
```

---

## Task 3: Modify ioperf_benchmark.c - Add -c CLI Option and Async Workers

**Files:**
- Modify: `lib/ioperf/ioperf.h` (for ioperf_opts update - already done in Task 1)
- Modify: `tools/ioperf_benchmark.c:26-37` (benchmark_opts struct)
- Modify: `tools/ioperf_benchmark.c:302-318` (print_usage)
- Modify: `tools/ioperf_benchmark.c:337-362` (main - option parsing)
- Modify: `tools/ioperf_benchmark.c:379-388` (main - disk creation)

- [ ] **Step 1: Add poller_cpus fields to benchmark_opts struct**

Read lines 26-37:
```c
struct benchmark_opts {
    uint64_t disk_size_mb;
    uint32_t block_size;
    uint32_t num_threads;
    uint32_t io_depth;
    uint32_t duration_sec;
    uint32_t read_latency_us;
    uint32_t write_latency_us;
    bool is_read_test;
    bool is_write_test;
    bool is_rand_test;
};
```

Add poller_cpus fields:
```c
struct benchmark_opts {
    uint64_t disk_size_mb;
    uint32_t block_size;
    uint32_t num_threads;
    uint32_t io_depth;
    uint32_t duration_sec;
    uint32_t read_latency_us;
    uint32_t write_latency_us;
    bool is_read_test;
    bool is_write_test;
    bool is_rand_test;
    uint32_t *poller_cpus;       /* NEW: CPU IDs for poller affinity */
    uint32_t  poller_cpus_count; /* NEW: length of poller_cpus array */
};
```

- [ ] **Step 2: Add -c option to print_usage**

Read lines 302-318:
```c
    printf("  -T <sec>        Test duration in seconds (default: %d)\n", DEFAULT_TEST_DURATION_SEC);
    printf("  -r <latency>    Read latency in microseconds (default: 0)\n");
    printf("  -w <latency>    Write latency in microseconds (default: 0)\n");
    printf("  --read          Run read test (sequential)\n");
    printf("  --write         Run write test (sequential)\n");
    printf("  --rand          Run random I/O test (read/write mixed)\n");
    printf("  -h, --help      Show this help\n");
```

Add -c option before -h:
```c
    printf("  -T <sec>        Test duration in seconds (default: %d)\n", DEFAULT_TEST_DURATION_SEC);
    printf("  -r <latency>    Read latency in microseconds (default: 0)\n");
    printf("  -w <latency>    Write latency in microseconds (default: 0)\n");
    printf("  --read          Run read test (sequential)\n");
    printf("  --write         Run write test (sequential)\n");
    printf("  --rand          Run random I/O test (read/write mixed)\n");
    printf("  -c <cpus>       Poller CPU list, e.g. 0,2,4,6 (default: none)\n");
    printf("  -h, --help      Show this help\n");
```

- [ ] **Step 3: Parse -c option in main()**

Read lines 337-362 - find the option parsing loop. Add after the rand test parsing:
```c
        } else if (strcmp(argv[i], "--rand") == 0) {
            opts.is_rand_test = true;
        } else if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
            /* Parse CPU list, e.g. "0,2,4,6" */
            char *cpulist = argv[++i];
            char *token = strtok(cpulist, ",");
            uint32_t cpus[32];  /* max 32 CPUs */
            uint32_t count = 0;
            while (token != NULL && count < 32) {
                cpus[count++] = atoi(token);
                token = strtok(NULL, ",");
            }
            if (count > 0) {
                opts.poller_cpus = calloc(count, sizeof(uint32_t));
                memcpy(opts.poller_cpus, cpus, count * sizeof(uint32_t));
                opts.poller_cpus_count = count;
            }
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
```

- [ ] **Step 4: Pass poller_cpus to ioperf_opts when creating disk**

Read lines 379-388:
```c
    struct ioperf_opts disk_opts = {
        .name = "benchmark_disk",
        .num_blocks = (opts.disk_size_mb * 1024 * 1024) / opts.block_size,
        .block_size = opts.block_size,
        .physical_block_size = opts.block_size,
        .num_threads = opts.num_threads,
        .read_latency_us = opts.read_latency_us,
        .write_latency_us = opts.write_latency_us,
        .enable_validation = false,
    };
```

Add poller_cpus:
```c
    struct ioperf_opts disk_opts = {
        .name = "benchmark_disk",
        .num_blocks = (opts.disk_size_mb * 1024 * 1024) / opts.block_size,
        .block_size = opts.block_size,
        .physical_block_size = opts.block_size,
        .num_threads = opts.num_threads,
        .read_latency_us = opts.read_latency_us,
        .write_latency_us = opts.write_latency_us,
        .enable_validation = false,
        .poller_cpus = opts.poller_cpus,           /* NEW */
        .poller_cpus_count = opts.poller_cpus_count, /* NEW */
    };
```

- [ ] **Step 5: Add free for poller_cpus at end of main()**

Read lines 472-476:
```c
    free(g_worker_threads);
    free(g_stats);
    ioperf_disk_destroy(g_disk);

    return 0;
```

Add poller_cpus free:
```c
    free(g_worker_threads);
    free(g_stats);
    ioperf_disk_destroy(g_disk);
    if (opts.poller_cpus) {
        free(opts.poller_cpus);
    }

    return 0;
```

- [ ] **Step 6: Commit**

```bash
git add tools/ioperf_benchmark.c && git commit -m "feat: add -c option for poller CPU affinity

- Add poller_cpus field to benchmark_opts struct
- Parse -c <cpus> option (e.g. 0,2,4,6)
- Pass poller_cpus to ioperf_opts when creating disk
- Free poller_cpus at cleanup"
```

---

## Task 4: Test and Validate

**Files:**
- Test: `tools/ioperf_benchmark.c` (manual test)
- Test: `test/ioperf_test.cc` (existing tests should still pass)

- [ ] **Step 1: Build the benchmark**

```bash
cd /home/claudeuser/ioperftest && make clean && make benchmark
```

Expected: Builds without errors

- [ ] **Step 2: Run test without latency (should be fast)**

```bash
./ioperf_benchmark --read -t 4 -T 5
```

Expected: Runs successfully, ~15M IOPS without latency

- [ ] **Step 3: Run test with latency and CPU affinity**

```bash
./ioperf_benchmark --read -t 4 -T 5 -r 100 -c 0
```

Expected: Runs successfully, poller bound to CPU 0

- [ ] **Step 4: Run existing unit tests**

```bash
cd /home/claudeuser/ioperftest/build && make test
```

Expected: All existing tests pass

- [ ] **Step 5: Commit**

```bash
git add -a && git commit -m "test: validate async ioperf with CPU affinity

- Benchmark runs without latency at expected IOPS
- Benchmark runs with latency and CPU affinity
- All unit tests pass""
```

---

## Validation Checklist

- [ ] Poller threads bound to specified CPUs (`ps -o cpu_list -L -p <pid>`)
- [ ] Busy-polling confirmed (no nanosleep in poller code path)
- [ ] Worker submits IO and returns immediately (no spin-wait on completion)
- [ ] Latency injection works correctly (100us delay → poller processes after 100us)
- [ ] No locks used in IO path (only atomic operations)
- [ ] All existing tests pass

---

## Implementation Complete

When all tasks are done and validated, the architecture will be:

```
┌─────────────────────────────────────────────────────────┐
│ Worker Thread (any core)                               │
│   └─ ioperf_submit_io() → returns immediately          │
│              │                                         │
│              ▼                                         │
│   channel->wait_queue (SPSC, lock-free)                │
└─────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│ Poller Thread (CPU 0, busy-poll, no sleep)             │
│   └─ while(poller_running) { busy-poll wait_queue }   │
│              │                                         │
│              │ latency expired (e.g. 100us)            │
│              ▼                                         │
│   ioperf_process_io() → callback() → update stats      │
└─────────────────────────────────────────────────────────┘
```
