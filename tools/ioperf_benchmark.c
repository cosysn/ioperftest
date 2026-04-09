/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2024
 * IOperf async benchmark tool
 *
 * Architecture:
 *   Worker: allocate req -> enqueue to lock-free ring -> return immediately
 *   Poller: busy-poll ring -> if IO ready (delay exceeded) -> process
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <stdatomic.h>

#define DEFAULT_DISK_SIZE_MB   64
#define DEFAULT_BLOCK_SIZE     512
#define DEFAULT_NUM_THREADS    4
#define DEFAULT_IO_DEPTH       128
#define DEFAULT_TEST_DURATION_SEC 10
#define RING_SIZE             4096

/* Options */
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
    uint32_t *poller_cpus;
    uint32_t  poller_cpus_count;
};

/* Per-thread statistics */
struct thread_stats {
    uint64_t io_completed;
    uint64_t bytes_completed;
    uint64_t total_latency_ns;
    uint64_t min_latency_ns;
    uint64_t max_latency_ns;
};

/* IO request - one per slot in ring */
struct io_request {
    uint64_t submit_ns;
    uint64_t offset_blocks;
    uint32_t num_blocks;
    uint32_t io_size;
    struct thread_stats *stats;
    struct io_request *next;  /* for delay list */

    /* Fields for fill_all_fields simulation */
    uint64_t field_001, field_002, field_003, field_004, field_005;
    uint64_t field_006, field_007, field_008, field_009, field_010;
    uint64_t field_011, field_012, field_013, field_014, field_015;
    uint64_t field_016, field_017, field_018, field_019, field_020;
    uint64_t field_021, field_022, field_023, field_024, field_025;
    uint64_t field_026, field_027, field_028, field_029, field_030;
};

/* Memory pool for io_request (lock-free stack) */
struct mem_pool {
    struct io_request *requests;  /* pre-allocated array */
    _Atomic uintptr_t free_stack;  /* lock-free stack head (as uintptr_t) */
    uint32_t size;
};

/* Lock-free ring buffer (SPSC) */
struct ring {
    struct io_request **slots;
    uint32_t size;
    _Atomic uint32_t head;  /* producer writes here */
    _Atomic uint32_t tail;  /* consumer reads here */

    /* Per-ring memory pool for this channel */
    struct mem_pool pool;

    /* Rate limiting (token bucket) */
    uint64_t token_bucket;
    uint64_t last_time;
    uint64_t max_bandwidth_bytes;
};

/* Delay list per poller (sorted by submit time) */
struct delay_list {
    struct io_request *head;
    struct io_request *tail;
    uint32_t count;
};

static inline uint64_t get_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

/* Forward declarations for pool functions */
static void pool_init(struct mem_pool *pool, uint32_t size);
static void pool_destroy(struct mem_pool *pool);

/* Ring init */
static void ring_init(struct ring *r, uint32_t size) {
    r->slots = calloc(size, sizeof(struct io_request *));
    r->size = size;
    atomic_init(&r->head, 0);
    atomic_init(&r->tail, 0);
    /* Initialize per-ring memory pool */
    pool_init(&r->pool, size);
    /* Initialize rate limiting */
    r->token_bucket = 0;
    r->last_time = 0;
    r->max_bandwidth_bytes = 0;  /* 0 = unlimited */
}

/* Ring destroy */
static void ring_destroy(struct ring *r) {
    free(r->slots);
    pool_destroy(&r->pool);
}

/* Ring is empty */
static bool ring_empty(struct ring *r) {
    return atomic_load(&r->head) == atomic_load(&r->tail);
}

/* Ring enqueue (非阻塞) - 返回 true 成功 */
static bool ring_enqueue(struct ring *r, struct io_request *req) {
    uint32_t head = atomic_load(&r->head);
    uint32_t next_head = (head + 1) % r->size;

    if (next_head == atomic_load(&r->tail)) {
        return false;  /* 队列满 */
    }

    r->slots[head] = req;
    atomic_store(&r->head, next_head);
    return true;
}

/* Ring dequeue (非阻塞) - 返回 NULL 表示空 */
static struct io_request *ring_dequeue(struct ring *r) {
    if (ring_empty(r)) {
        return NULL;
    }

    uint32_t tail = atomic_load(&r->tail);
    struct io_request *req = r->slots[tail];
    atomic_store(&r->tail, (tail + 1) % r->size);
    return req;
}

/* ============================================================================
 * fill_all_fields - 模拟真实内存访问和工作负载
 * ============================================================================ */
static void
fill_all_fields(struct io_request *req)
{
    req->field_001 = req->offset_blocks;
    req->field_002 = req->num_blocks;
    req->field_003 = req->io_size;
    req->field_004 = req->field_001 + req->field_002;
    req->field_005 = req->field_003 + req->field_004;
    req->field_006 = req->field_001 * 2;
    req->field_007 = req->field_002 * 2;
    req->field_008 = req->field_003 * 2;
    req->field_009 = req->field_004 * 2;
    req->field_010 = req->field_005 * 2;
    req->field_011 = req->field_001 - req->field_002;
    req->field_012 = req->field_003 - req->field_004;
    req->field_013 = req->field_005 - req->field_006;
    req->field_014 = req->field_007 - req->field_008;
    req->field_015 = req->field_009 - req->field_010;
    req->field_016 = req->field_001 & 0xFF;
    req->field_017 = req->field_002 & 0xFF;
    req->field_018 = req->field_003 & 0xFF;
    req->field_019 = req->field_004 & 0xFF;
    req->field_020 = req->field_005 & 0xFF;
    req->field_021 = req->field_006 | 0xFF;
    req->field_022 = req->field_007 | 0xFF;
    req->field_023 = req->field_008 | 0xFF;
    req->field_024 = req->field_009 | 0xFF;
    req->field_025 = req->field_010 | 0xFF;
    req->field_026 = req->field_001 ^ req->field_002;
    req->field_027 = req->field_003 ^ req->field_004;
    req->field_028 = req->field_005 ^ req->field_006;
    req->field_029 = req->field_007 ^ req->field_008;
    req->field_030 = req->field_009 ^ req->field_010;
}

/* ============================================================================
 * Rate limiting (token bucket)
 * ============================================================================ */

/* Get CPU ticks per second */
static uint64_t
get_ticks_per_second(void)
{
    return 1000000000ULL;  /* nanoseconds */
}

/* Rate limit check - token bucket algorithm */
static bool
rate_limit_check(struct ring *ring, uint64_t io_size)
{
    uint64_t now = get_time_ns();
    uint64_t ticks_per_sec = get_ticks_per_second();

    /* Initialize on first call */
    if (ring->last_time == 0) {
        ring->last_time = now;
        ring->token_bucket = ring->max_bandwidth_bytes;
    }

    uint64_t elapsed = now - ring->last_time;

    if (elapsed > 0) {
        /* Refill token bucket */
        uint64_t tokens_to_add = (ring->max_bandwidth_bytes / ticks_per_sec) * elapsed;
        ring->token_bucket += tokens_to_add;
        ring->last_time = now;
    }

    /* Check rate limit */
    if (ring->token_bucket >= io_size) {
        ring->token_bucket -= io_size;
        return true;
    }

    return false;
}

/* Delay list init */
static void delay_list_init(struct delay_list *dl) {
    dl->head = NULL;
    dl->tail = NULL;
    dl->count = 0;
}

/* Memory pool init - pre-allocate all io_requests */
static void pool_init(struct mem_pool *pool, uint32_t size) {
    pool->size = size;
    pool->requests = calloc(size, sizeof(struct io_request));
    atomic_store(&pool->free_stack, (uintptr_t)NULL);

    /* Build free stack - each request points to next */
    for (uint32_t i = 0; i < size; i++) {
        pool->requests[i].next = (i + 1 < size) ? &pool->requests[i + 1] : NULL;
    }
    atomic_store(&pool->free_stack, (uintptr_t)&pool->requests[0]);
}

/* Memory pool destroy */
static void pool_destroy(struct mem_pool *pool) {
    free(pool->requests);
}

/* Pool alloc - pop from lock-free stack, O(1) */
static struct io_request *pool_alloc(struct mem_pool *pool) {
    uintptr_t head = atomic_load(&pool->free_stack);
    while (head != (uintptr_t)NULL) {
        struct io_request *req = (struct io_request *)head;
        uintptr_t next = (uintptr_t)req->next;
        if (atomic_compare_exchange_weak(&pool->free_stack, &head, next)) {
            /* Re-init the request */
            req->submit_ns = 0;
            req->offset_blocks = 0;
            req->num_blocks = 0;
            req->io_size = 0;
            req->stats = NULL;
            req->next = NULL;
            return req;
        }
        /* CAS failed, retry with new head */
    }
    return NULL;  /* pool exhausted */
}

/* Pool free - push back to lock-free stack, O(1) */
static void pool_free(struct mem_pool *pool, struct io_request *req) {
    uintptr_t head = atomic_load(&pool->free_stack);
    while (1) {
        req->next = (struct io_request *)head;
        if (atomic_compare_exchange_weak(&pool->free_stack, &head, (uintptr_t)req)) {
            return;
        }
        /* CAS failed, head is updated with current stack head, retry */
    }
}

/* Add to delay list (FIFO, O(1)) */
static void delay_list_add(struct delay_list *dl, struct io_request *req) {
    req->next = NULL;
    if (dl->tail) {
        dl->tail->next = req;
        dl->tail = req;
    } else {
        dl->head = dl->tail = req;
    }
    dl->count++;
}

/* Pop head if delay exceeded */
static struct io_request *delay_list_pop_ready(struct delay_list *dl,
                                               uint64_t now, uint64_t delay_ns) {
    if (dl->head == NULL) {
        return NULL;
    }

    if (now - dl->head->submit_ns >= delay_ns) {
        struct io_request *req = dl->head;
        dl->head = req->next;
        if (dl->head == NULL) {
            dl->tail = NULL;
        }
        dl->count--;
        return req;
    }
    return NULL;
}

/* Global state */
static volatile bool g_running;
static volatile bool g_draining;
static pthread_t *g_worker_threads;
static pthread_t *g_poller_threads;
static struct thread_stats *g_stats;
static uint32_t g_num_threads;
static uint64_t g_start_ns;
static struct ring *g_rings;
static struct delay_list *g_delay_lists;
static uint64_t g_read_delay_ns;
static uint64_t g_write_delay_ns;
static _Atomic uint64_t g_total_processed;
static _Atomic uint32_t g_in_flight;  /* current in-flight IO count */
static uint32_t g_target_depth;       /* target IO depth per worker */

/* Worker thread - 维持固定队列深度 */
static void *
worker_thread(void *arg)
{
    uint32_t worker_id = *(uint32_t *)arg;
    struct ring *ring = &g_rings[worker_id];
    uint64_t offset = 0;

    while (g_running) {
        /* 等待直到 in_flight < target_depth */
        while (atomic_load(&g_in_flight) >= g_target_depth) {
            /* spin-wait */
        }

        /* Rate limit check (if bandwidth configured) */
        if (ring->max_bandwidth_bytes > 0) {
            while (!rate_limit_check(ring, 512)) {
                /* spin-wait for token bucket */
            }
        }

        /* 从内存池分配请求 */
        struct io_request *req = pool_alloc(&ring->pool);
        if (req == NULL) {
            continue;  /* 池空，暂跳过 */
        }
        req->submit_ns = get_time_ns();
        req->offset_blocks = offset;
        req->num_blocks = 1;
        req->io_size = 512;
        req->stats = &g_stats[worker_id];

        atomic_fetch_add(&g_in_flight, 1);

        /* 入队，如果满则丢弃（不应该发生） */
        if (!ring_enqueue(ring, req)) {
            pool_free(&ring->pool, req);  /* 队列满，丢弃 */
            atomic_fetch_sub(&g_in_flight, 1);
        }

        /* 推进 offset */
        offset += g_num_threads * 512;
        if (offset >= 64 * 1024 * 1024) {
            offset = 0;
        }
    }

    return NULL;
}

/* Poller thread - 纯轮询，无 sleep 无 yield */
static void *
poller_thread(void *arg)
{
    uint32_t poller_id = *(uint32_t *)arg;
    struct ring *ring = &g_rings[poller_id];
    struct delay_list *dl = &g_delay_lists[poller_id];

    while (g_running || dl->count > 0) {
        /* 1. 从 ring 取 IO 加入 delay list */
        struct io_request *req;
        while ((req = ring_dequeue(ring)) != NULL) {
            delay_list_add(dl, req);
        }

        /* 2. 处理 delay list 中 ready 的 IO (now 在循环内刷新) */
        struct io_request *ready;
        while ((ready = delay_list_pop_ready(dl, get_time_ns(), g_read_delay_ns)) != NULL) {
            uint64_t now = get_time_ns();
            uint64_t latency = now - ready->submit_ns;

            /* 模拟内存访问和工作负载 */
            fill_all_fields(ready);

            /* 统计 - 只在运行阶段，非 drain 阶段 */
            atomic_fetch_add(&g_total_processed, 1);
            atomic_fetch_sub(&g_in_flight, 1);
            ready->stats->io_completed++;
            ready->stats->bytes_completed += ready->io_size;
            ready->stats->total_latency_ns += latency;
            if (ready->stats->min_latency_ns == 0 || latency < ready->stats->min_latency_ns) {
                ready->stats->min_latency_ns = latency;
            }
            if (latency > ready->stats->max_latency_ns) {
                ready->stats->max_latency_ns = latency;
            }

            pool_free(&ring->pool, ready);
        }

        /* 3. 纯轮询，不让出 CPU */
    }

    /* Drain remaining */
    while (1) {
        struct io_request *req;
        while ((req = ring_dequeue(ring)) != NULL) {
            delay_list_add(dl, req);
        }

        uint64_t now = get_time_ns();
        struct io_request *ready;
        while ((ready = delay_list_pop_ready(dl, now, 0)) != NULL) {
            atomic_fetch_sub(&g_in_flight, 1);
            ready->stats->io_completed++;
            ready->stats->bytes_completed += ready->io_size;
            pool_free(&ring->pool, ready);
        }

        if (ring_empty(ring) && dl->count == 0) {
            break;
        }
    }

    return NULL;
}

/* Stats printer */
static void *
stats_printer(void *arg)
{
    uint32_t duration = *(uint32_t *)arg;
    uint32_t sec = 1;

    printf("%-6s %10s %10s %10s %10s %10s\n",
           "Time", "IOPS", "r/s", "w/s", "MB/s", "lat(us)");
    printf("%-6s %10s %10s %10s %10s %10s\n",
           "------", "----------", "----------", "----------", "----------", "----------");

    while (g_running) {
        sleep(1);

        uint64_t total_io = 0, total_bytes = 0, total_lat_ns = 0;
        for (uint32_t i = 0; i < g_num_threads; i++) {
            total_io += g_stats[i].io_completed;
            total_bytes += g_stats[i].bytes_completed;
            total_lat_ns += g_stats[i].total_latency_ns;
        }

        static uint64_t last_io = 0, last_bytes = 0, last_lat_ns = 0;
        uint64_t interval_io = total_io - last_io;
        uint64_t interval_bytes = total_bytes - last_bytes;
        uint64_t interval_lat_ns = total_lat_ns - last_lat_ns;
        last_io = total_io;
        last_bytes = total_bytes;
        last_lat_ns = total_lat_ns;

        double iops = (double)interval_io;
        double mbps = (double)interval_bytes / (1024.0 * 1024.0);
        double avg_lat = (interval_io > 0) ? (double)interval_lat_ns / interval_io / 1000.0 : 0;

        printf("%-6u %10.0f %10.0f %10.0f %10.2f %10.2f\n",
               sec, iops, iops, 0.0, mbps, avg_lat);
        sec++;
        if (sec >= duration) break;
    }
    return NULL;
}

static void print_usage(const char *prog) {
    printf("Usage: %s [options]\n", prog);
    printf("\nOptions:\n");
    printf("  -t <threads>    Threads (default: %d)\n", DEFAULT_NUM_THREADS);
    printf("  -T <sec>        Duration (default: %d)\n", DEFAULT_TEST_DURATION_SEC);
    printf("  -r <us>         Read latency (default: 100)\n");
    printf("  -d <depth>      IO depth per worker (default: 128)\n");
    printf("  -c <cpus>       Poller CPUs (e.g. 0,2,4,6)\n");
    printf("  -h, --help      Help\n");
}

int main(int argc, char *argv[])
{
    struct benchmark_opts opts = {
        .num_threads = DEFAULT_NUM_THREADS,
        .duration_sec = DEFAULT_TEST_DURATION_SEC,
        .read_latency_us = 100,
        .write_latency_us = 200,
        .poller_cpus = NULL,
        .poller_cpus_count = 0,
        .io_depth = 128,
    };

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) opts.num_threads = atoi(argv[++i]);
        else if (strcmp(argv[i], "-T") == 0 && i + 1 < argc) opts.duration_sec = atoi(argv[++i]);
        else if (strcmp(argv[i], "-r") == 0 && i + 1 < argc) opts.read_latency_us = atoi(argv[++i]);
        else if (strcmp(argv[i], "-w") == 0 && i + 1 < argc) opts.write_latency_us = atoi(argv[++i]);
        else if (strcmp(argv[i], "-d") == 0 && i + 1 < argc) opts.io_depth = atoi(argv[++i]);
        else if (strcmp(argv[i], "--read") == 0) opts.is_read_test = true;
        else if (strcmp(argv[i], "--write") == 0) opts.is_write_test = true;
        else if (strcmp(argv[i], "--rand") == 0) opts.is_rand_test = true;
        else if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
            char *p = strdup(argv[++i]);
            char *t = strtok(p, ",");
            uint32_t cpus[32], n = 0;
            while (t && n < 32) cpus[n++] = atoi(t), t = strtok(NULL, ",");
            free(p);
            if (n > 0) {
                opts.poller_cpus = calloc(n, sizeof(uint32_t));
                memcpy(opts.poller_cpus, cpus, n * sizeof(uint32_t));
                opts.poller_cpus_count = n;
            }
        }
        else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        }
    }

    printf("=== IOperf Async Benchmark ===\n");
    printf("Threads: %u, Duration: %u sec, IO depth: %u\n", opts.num_threads, opts.duration_sec, opts.io_depth);
    printf("Read latency: %u us, Write latency: %u us\n", opts.read_latency_us, opts.write_latency_us);
    if (opts.poller_cpus_count > 0) {
        printf("Poller CPUs: ");
        for (uint32_t i = 0; i < opts.poller_cpus_count; i++) printf("%u ", opts.poller_cpus[i]);
        printf("\n");
    }

    g_num_threads = opts.num_threads;
    g_running = true;
    g_draining = false;
    g_read_delay_ns = opts.read_latency_us * 1000ULL;
    g_write_delay_ns = opts.write_latency_us * 1000ULL;
    g_target_depth = opts.io_depth;
    atomic_store(&g_in_flight, 0);

    g_stats = calloc(opts.num_threads, sizeof(struct thread_stats));
    for (uint32_t i = 0; i < opts.num_threads; i++) g_stats[i].min_latency_ns = UINT64_MAX;

    g_rings = calloc(opts.num_threads, sizeof(struct ring));
    g_delay_lists = calloc(opts.num_threads, sizeof(struct delay_list));
    for (uint32_t i = 0; i < opts.num_threads; i++) {
        ring_init(&g_rings[i], RING_SIZE);
        delay_list_init(&g_delay_lists[i]);
    }

    g_worker_threads = calloc(opts.num_threads, sizeof(pthread_t));
    g_poller_threads = calloc(opts.num_threads, sizeof(pthread_t));
    uint32_t *worker_ids = calloc(opts.num_threads, sizeof(uint32_t));
    uint32_t *poller_ids = calloc(opts.num_threads, sizeof(uint32_t));

    g_start_ns = get_time_ns();

    pthread_t printer_thread;
    pthread_create(&printer_thread, NULL, stats_printer, &opts.duration_sec);

    for (uint32_t i = 0; i < opts.num_threads; i++) {
        worker_ids[i] = i;
        poller_ids[i] = i;

        pthread_create(&g_poller_threads[i], NULL, poller_thread, &poller_ids[i]);
        if (opts.poller_cpus_count > 0 && i < opts.poller_cpus_count) {
            cpu_set_t cs; CPU_ZERO(&cs); CPU_SET(opts.poller_cpus[i], &cs);
            pthread_setaffinity_np(g_poller_threads[i], sizeof(cs), &cs);
        }

        pthread_create(&g_worker_threads[i], NULL, worker_thread, &worker_ids[i]);
    }

    sleep(opts.duration_sec);
    g_running = false;
    g_draining = true;

    for (uint32_t i = 0; i < opts.num_threads; i++) {
        pthread_join(g_worker_threads[i], NULL);
        pthread_join(g_poller_threads[i], NULL);
    }
    pthread_join(printer_thread, NULL);

    double total_sec = (double)(get_time_ns() - g_start_ns) / 1e9;

    uint64_t total_io = 0, total_bytes = 0, total_lat = 0, min_lat = UINT64_MAX, max_lat = 0;
    for (uint32_t i = 0; i < opts.num_threads; i++) {
        total_io += g_stats[i].io_completed;
        total_bytes += g_stats[i].bytes_completed;
        total_lat += g_stats[i].total_latency_ns;
        if (g_stats[i].min_latency_ns < min_lat) min_lat = g_stats[i].min_latency_ns;
        if (g_stats[i].max_latency_ns > max_lat) max_lat = g_stats[i].max_latency_ns;
    }

    printf("\n=== Final Results ===\n");
    printf("Total processed (atomic): %lu\n", atomic_load(&g_total_processed));
    printf("Total IO: %lu, Bytes: %lu (%.1f MB)\n", total_io, total_bytes, (double)total_bytes / 1024/1024);
    printf("Duration: %.2f sec\n", total_sec);
    printf("IOPS: %.2f\n", (double)total_io / total_sec);
    printf("Bandwidth: %.2f MB/s\n", (double)total_bytes / 1024/1024 / total_sec);
    if (total_io > 0) {
        printf("Avg latency: %.2f us\n", (double)total_lat / total_io / 1000.0);
        printf("Min latency: %.2f us\n", (double)min_lat / 1000.0);
        printf("Max latency: %.2f us\n", (double)max_lat / 1000.0);
    }

    free(g_stats);
    free(g_worker_threads);
    free(g_poller_threads);
    free(worker_ids);
    free(poller_ids);
    for (uint32_t i = 0; i < opts.num_threads; i++) {
        ring_destroy(&g_rings[i]);
    }
    free(g_rings);
    free(g_delay_lists);
    if (opts.poller_cpus) free(opts.poller_cpus);

    return 0;
}
