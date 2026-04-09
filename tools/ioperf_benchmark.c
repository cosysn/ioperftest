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
};

/* Lock-free ring buffer (SPSC) */
struct ring {
    struct io_request **slots;
    uint32_t size;
    _Atomic uint32_t head;  /* producer writes here */
    _Atomic uint32_t tail;  /* consumer reads here */
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

/* Ring init */
static void ring_init(struct ring *r, uint32_t size) {
    r->slots = calloc(size, sizeof(struct io_request *));
    r->size = size;
    atomic_init(&r->head, 0);
    atomic_init(&r->tail, 0);
}

/* Ring destroy */
static void ring_destroy(struct ring *r) {
    free(r->slots);
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

/* Delay list init */
static void delay_list_init(struct delay_list *dl) {
    dl->head = NULL;
    dl->tail = NULL;
    dl->count = 0;
}

/* Delay list destroy */
static void delay_list_destroy(struct delay_list *dl) {
    struct io_request *r = dl->head;
    while (r) {
        struct io_request *next = r->next;
        free(r);
        r = next;
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

/* Worker thread - 纯异步，提交后立即返回 */
static void *
worker_thread(void *arg)
{
    uint32_t worker_id = *(uint32_t *)arg;
    struct ring *ring = &g_rings[worker_id];
    uint64_t offset = 0;

    while (g_running) {
        /* 分配请求 */
        struct io_request *req = calloc(1, sizeof(*req));
        req->submit_ns = get_time_ns();
        req->offset_blocks = offset;
        req->num_blocks = 1;
        req->io_size = 512;
        req->stats = &g_stats[worker_id];

        /* 入队，如果满则丢弃（不应该发生） */
        if (!ring_enqueue(ring, req)) {
            free(req);  /* 队列满，丢弃 */
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

            /* 模拟内存访问 */
            volatile uint64_t sum = 0;
            for (int i = 0; i < 128; i++) {
                sum += ready->offset_blocks + i;
            }
            (void)sum;

            /* 统计 - 只在运行阶段，非 drain 阶段 */
            atomic_fetch_add(&g_total_processed, 1);
            ready->stats->io_completed++;
            ready->stats->bytes_completed += ready->io_size;
            ready->stats->total_latency_ns += latency;
            if (ready->stats->min_latency_ns == 0 || latency < ready->stats->min_latency_ns) {
                ready->stats->min_latency_ns = latency;
            }
            if (latency > ready->stats->max_latency_ns) {
                ready->stats->max_latency_ns = latency;
            }

            free(ready);
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
            ready->stats->io_completed++;
            ready->stats->bytes_completed += ready->io_size;
            free(ready);
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
    };

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) opts.num_threads = atoi(argv[++i]);
        else if (strcmp(argv[i], "-T") == 0 && i + 1 < argc) opts.duration_sec = atoi(argv[++i]);
        else if (strcmp(argv[i], "-r") == 0 && i + 1 < argc) opts.read_latency_us = atoi(argv[++i]);
        else if (strcmp(argv[i], "-w") == 0 && i + 1 < argc) opts.write_latency_us = atoi(argv[++i]);
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
    printf("Threads: %u, Duration: %u sec\n", opts.num_threads, opts.duration_sec);
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
        delay_list_destroy(&g_delay_lists[i]);
    }
    free(g_rings);
    free(g_delay_lists);
    if (opts.poller_cpus) free(opts.poller_cpus);

    return 0;
}
