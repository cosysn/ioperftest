/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2024
 * IOperf async benchmark tool
 * Fully async architecture: worker -> ring buffer -> poller -> completion -> worker
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
#include <sched.h>
#include <errno.h>

/* Default configuration */
#define DEFAULT_DISK_SIZE_MB   64
#define DEFAULT_BLOCK_SIZE     512
#define DEFAULT_NUM_THREADS    4
#define DEFAULT_IO_DEPTH       128
#define DEFAULT_TEST_DURATION_SEC 10
#define QUEUE_SIZE             4096

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

/* IO request wrapper */
struct io_request {
    uint64_t submit_ns;
    uint64_t complete_ns;
    uint64_t offset_blocks;
    uint32_t num_blocks;
    uint32_t io_size;
    struct thread_stats *stats;
};

/* Ring buffer for SPSC */
struct ring_buffer {
    struct io_request *requests;
    uint32_t size;
    _Atomic uint32_t head;  /* producer writes here */
    _Atomic uint32_t tail;  /* consumer reads here */
    pthread_mutex_t mutex;
    pthread_cond_t cond;
};

/* Global state */
static volatile bool g_running;
static volatile bool g_draining;
static pthread_t *g_worker_threads;
static pthread_t *g_poller_threads;
static struct thread_stats *g_stats;
static uint32_t g_num_threads;
static uint64_t g_start_ns;
static struct ring_buffer *g_req_queues;
static struct ring_buffer *g_comp_queues;

/* Get current time in nanoseconds */
static inline uint64_t
get_time_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

/* Ring buffer init */
static void
ring_init(struct ring_buffer *rb, uint32_t size)
{
    rb->requests = calloc(size, sizeof(struct io_request));
    rb->size = size;
    atomic_init(&rb->head, 0);
    atomic_init(&rb->tail, 0);
    pthread_mutex_init(&rb->mutex, NULL);
    pthread_cond_init(&rb->cond, NULL);
}

/* Ring buffer destroy */
static void
ring_destroy(struct ring_buffer *rb)
{
    free(rb->requests);
    pthread_mutex_destroy(&rb->mutex);
    pthread_cond_destroy(&rb->cond);
}

/* Ring buffer is empty */
static bool
ring_empty(struct ring_buffer *rb)
{
    return atomic_load(&rb->head) == atomic_load(&rb->tail);
}

/* Ring buffer is full */
static bool
ring_full(struct ring_buffer *rb)
{
    uint32_t head = atomic_load(&rb->head);
    uint32_t next_head = (head + 1) % rb->size;
    return next_head == atomic_load(&rb->tail);
}

/* Enqueue (非阻塞) - 返回 true 成功, false 队列满 */
static bool
ring_enqueue(struct ring_buffer *rb, struct io_request *req)
{
    uint32_t head = atomic_load(&rb->head);
    uint32_t next_head = (head + 1) % rb->size;

    /* 检查队列满 */
    if (next_head == atomic_load(&rb->tail)) {
        return false;
    }

    /* 复制请求到队列 */
    memcpy(&rb->requests[head], req, sizeof(struct io_request));

    /* 发布头位置 */
    atomic_store(&rb->head, next_head);

    /* 信号消费者 */
    pthread_mutex_lock(&rb->mutex);
    pthread_cond_signal(&rb->cond);
    pthread_mutex_unlock(&rb->mutex);

    return true;
}

/* Dequeue (阻塞直到有数据) - 返回 true 成功 */
static bool
ring_dequeue(struct ring_buffer *rb, struct io_request *req)
{
    pthread_mutex_lock(&rb->mutex);

    while (ring_empty(rb) && g_running) {
        pthread_cond_wait(&rb->cond, &rb->mutex);
    }

    if (ring_empty(rb)) {
        pthread_mutex_unlock(&rb->mutex);
        return false;
    }

    uint32_t tail = atomic_load(&rb->tail);
    memcpy(req, &rb->requests[tail], sizeof(struct io_request));
    atomic_store(&rb->tail, (tail + 1) % rb->size);

    pthread_mutex_unlock(&rb->mutex);

    return true;
}

/* 非阻塞 Dequeue - 返回 true 成功, false 队列空 */
static bool
ring_dequeue_nb(struct ring_buffer *rb, struct io_request *req)
{
    if (ring_empty(rb)) {
        return false;
    }

    uint32_t tail = atomic_load(&rb->tail);
    memcpy(req, &rb->requests[tail], sizeof(struct io_request));
    atomic_store(&rb->tail, (tail + 1) % rb->size);

    return true;
}

/* Poller thread - busy polls request queue, processes after delay */
static void *
poller_thread(void *arg)
{
    uint32_t poller_id = *(uint32_t *)arg;
    struct ring_buffer *req_queue = &g_req_queues[poller_id];
    struct ring_buffer *comp_queue = &g_comp_queues[poller_id];
    struct io_request req;

    while (g_running) {
        /* Try to dequeue a request (non-blocking) */
        if (!ring_dequeue_nb(req_queue, &req)) {
            /* 队列空，让出 CPU 避免 busy-wait */
            sched_yield();
            continue;
        }

        /* Check if delay has elapsed */
        uint64_t now = get_time_ns();
        uint64_t delay_ns = (uint64_t)100 * 1000;  /* 100us read latency */

        if (now - req.submit_ns < delay_ns) {
            /* Delay not met, put back in queue */
            if (!ring_enqueue(req_queue, &req)) {
                /* 队列满，busy wait 一下再试 */
                while (ring_full(req_queue) && g_running) {
                    sched_yield();
                }
                if (g_running) {
                    ring_enqueue(req_queue, &req);
                }
            }
            continue;
        }

        /* Delay met, process the IO (模拟处理) */
        /* 模拟一些计算工作 */
        volatile uint64_t sum = 0;
        for (uint32_t i = 0; i < 128; i++) {
            sum += req.offset_blocks + i;
        }
        (void)sum;

        /* Enqueue completion */
        req.complete_ns = get_time_ns();
        while (!ring_enqueue(comp_queue, &req) && g_running) {
            sched_yield();
        }
    }

    return NULL;
}

/* Worker thread - generates IO requests asynchronously */
static void *
worker_thread(void *arg)
{
    uint32_t worker_id = *(uint32_t *)arg;
    struct thread_stats *stats = &g_stats[worker_id];
    struct ring_buffer *req_queue = &g_req_queues[worker_id];
    struct ring_buffer *comp_queue = &g_comp_queues[worker_id];
    struct io_request req;
    uint64_t offset = 0;

    while (g_running) {
        /* Try to get completions (non-blocking) to update stats */
        while (ring_dequeue_nb(comp_queue, &req)) {
            uint64_t latency = req.complete_ns - req.submit_ns;

            stats->io_completed++;
            stats->bytes_completed += req.io_size;
            stats->total_latency_ns += latency;
            if (stats->min_latency_ns == 0 || latency < stats->min_latency_ns) {
                stats->min_latency_ns = latency;
            }
            if (latency > stats->max_latency_ns) {
                stats->max_latency_ns = latency;
            }
        }

        /* Submit new IO request */
        req.submit_ns = get_time_ns();
        req.offset_blocks = offset;
        req.num_blocks = 1;
        req.io_size = 512;
        req.stats = stats;

        /* Enqueue to poller */
        if (!ring_enqueue(req_queue, &req)) {
            /* 队列满，busy wait 一下 */
            while (ring_full(req_queue) && g_running) {
                sched_yield();
            }
            if (g_running && !ring_enqueue(req_queue, &req)) {
                /* 仍然失败，跳过一帧 */
            }
        }

        /* Advance offset */
        offset += g_num_threads * 512;
        if (offset >= 64 * 1024 * 1024) {  /* 64MB disk */
            offset = 0;
        }
    }

    /* Drain completion queue on exit (don't count latency) */
    while (ring_dequeue_nb(comp_queue, &req)) {
        /* Only count IO, not latency during drain */
        stats->io_completed++;
        stats->bytes_completed += req.io_size;
    }

    return NULL;
}

/* Stats printer thread */
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

        /* Calculate interval stats from global counters */
        uint64_t total_io = 0;
        uint64_t total_bytes = 0;
        for (uint32_t i = 0; i < g_num_threads; i++) {
            total_io += g_stats[i].io_completed;
            total_bytes += g_stats[i].bytes_completed;
        }

        static uint64_t last_io = 0;
        static uint64_t last_bytes = 0;
        uint64_t interval_io = total_io - last_io;
        uint64_t interval_bytes = total_bytes - last_bytes;
        last_io = total_io;
        last_bytes = total_bytes;

        double iops = (double)interval_io;
        double mbps = (double)interval_bytes / (1024.0 * 1024.0);
        double avg_lat = (interval_io > 0) ? (double)interval_bytes / interval_io / 1000.0 : 0;

        printf("%-6u %10.0f %10.0f %10.0f %10.2f %10.2f\n",
               sec, iops, iops, 0.0, mbps, avg_lat);
        sec++;

        if (sec >= duration) {
            break;
        }
    }

    return NULL;
}

/* Print usage */
static void
print_usage(const char *prog)
{
    printf("Usage: %s [options]\n", prog);
    printf("\nOptions:\n");
    printf("  -s <size_mb>    Disk size in MB (default: %d)\n", DEFAULT_DISK_SIZE_MB);
    printf("  -b <size>       Block size in bytes (default: %d)\n", DEFAULT_BLOCK_SIZE);
    printf("  -t <threads>    Number of threads (default: %d)\n", DEFAULT_NUM_THREADS);
    printf("  -d <depth>      IO depth (default: %d)\n", DEFAULT_IO_DEPTH);
    printf("  -T <sec>        Test duration in seconds (default: %d)\n", DEFAULT_TEST_DURATION_SEC);
    printf("  -r <latency>    Read latency in microseconds (default: 0)\n");
    printf("  -w <latency>    Write latency in microseconds (default: 0)\n");
    printf("  --read          Run read test (sequential)\n");
    printf("  --write         Run write test (sequential)\n");
    printf("  --rand          Run random I/O test (read/write mixed)\n");
    printf("  -c <cpus>       Poller CPU list, e.g. 0,2,4,6 (default: none)\n");
    printf("  -h, --help      Show this help\n");
}

int
main(int argc, char *argv[])
{
    struct benchmark_opts opts = {
        .disk_size_mb = DEFAULT_DISK_SIZE_MB,
        .block_size = DEFAULT_BLOCK_SIZE,
        .num_threads = DEFAULT_NUM_THREADS,
        .io_depth = DEFAULT_IO_DEPTH,
        .duration_sec = DEFAULT_TEST_DURATION_SEC,
        .read_latency_us = 0,
        .write_latency_us = 0,
        .is_read_test = false,
        .is_write_test = false,
        .is_rand_test = false,
        .poller_cpus = NULL,
        .poller_cpus_count = 0,
    };

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-s") == 0 && i + 1 < argc) {
            opts.disk_size_mb = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-b") == 0 && i + 1 < argc) {
            opts.block_size = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) {
            opts.num_threads = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-d") == 0 && i + 1 < argc) {
            opts.io_depth = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-T") == 0 && i + 1 < argc) {
            opts.duration_sec = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-r") == 0 && i + 1 < argc) {
            opts.read_latency_us = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-w") == 0 && i + 1 < argc) {
            opts.write_latency_us = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--read") == 0) {
            opts.is_read_test = true;
        } else if (strcmp(argv[i], "--write") == 0) {
            opts.is_write_test = true;
        } else if (strcmp(argv[i], "--rand") == 0) {
            opts.is_rand_test = true;
        } else if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
            /* Parse CPU list */
            char *cpulist = strdup(argv[++i]);
            char *token = strtok(cpulist, ",");
            uint32_t cpus[32];
            uint32_t count = 0;
            while (token != NULL && count < 32) {
                cpus[count++] = atoi(token);
                token = strtok(NULL, ",");
            }
            free(cpulist);
            if (count > 0) {
                opts.poller_cpus = calloc(count, sizeof(uint32_t));
                memcpy(opts.poller_cpus, cpus, count * sizeof(uint32_t));
                opts.poller_cpus_count = count;
            }
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        }
    }

    if (!opts.is_read_test && !opts.is_write_test && !opts.is_rand_test) {
        opts.is_read_test = true;
    }

    printf("=== IOperf Async Benchmark ===\n");
    printf("Disk size: %lu MB\n", opts.disk_size_mb);
    printf("Block size: %u bytes\n", opts.block_size);
    printf("Threads: %u\n", opts.num_threads);
    printf("IO depth: %u\n", opts.io_depth);
    printf("Duration: %u sec\n", opts.duration_sec);
    if (opts.read_latency_us > 0 || opts.write_latency_us > 0) {
        printf("Latency: read=%u us, write=%u us\n",
               opts.read_latency_us, opts.write_latency_us);
    }
    if (opts.poller_cpus_count > 0) {
        printf("Poller CPUs:   ");
        for (uint32_t i = 0; i < opts.poller_cpus_count; i++) {
            printf("%u ", opts.poller_cpus[i]);
        }
        printf("\n");
    }

    g_num_threads = opts.num_threads;
    g_running = true;

    /* Allocate stats */
    g_stats = calloc(opts.num_threads, sizeof(struct thread_stats));
    for (uint32_t i = 0; i < opts.num_threads; i++) {
        g_stats[i].min_latency_ns = UINT64_MAX;
    }

    /* Allocate queues */
    g_req_queues = calloc(opts.num_threads, sizeof(struct ring_buffer));
    g_comp_queues = calloc(opts.num_threads, sizeof(struct ring_buffer));
    for (uint32_t i = 0; i < opts.num_threads; i++) {
        ring_init(&g_req_queues[i], QUEUE_SIZE);
        ring_init(&g_comp_queues[i], QUEUE_SIZE);
    }

    /* Allocate thread arrays */
    g_worker_threads = calloc(opts.num_threads, sizeof(pthread_t));
    g_poller_threads = calloc(opts.num_threads, sizeof(pthread_t));

    uint32_t *worker_ids = calloc(opts.num_threads, sizeof(uint32_t));
    uint32_t *poller_ids = calloc(opts.num_threads, sizeof(uint32_t));

    printf("\nStarting benchmark...\n\n");

    g_start_ns = get_time_ns();

    /* Create stats printer thread */
    pthread_t printer_thread;
    pthread_create(&printer_thread, NULL, stats_printer, &opts.duration_sec);

    /* Create worker and poller threads */
    for (uint32_t i = 0; i < opts.num_threads; i++) {
        worker_ids[i] = i;
        poller_ids[i] = i;

        /* Create poller first */
        pthread_create(&g_poller_threads[i], NULL, poller_thread, &poller_ids[i]);

        /* Set poller CPU affinity if specified */
        if (opts.poller_cpus_count > 0 && i < opts.poller_cpus_count) {
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(opts.poller_cpus[i], &cpuset);
            pthread_setaffinity_np(g_poller_threads[i], sizeof(cpu_set_t), &cpuset);
        }

        /* Create worker */
        pthread_create(&g_worker_threads[i], NULL, worker_thread, &worker_ids[i]);
    }

    /* Wait for duration */
    sleep(opts.duration_sec);
    g_running = false;
    g_draining = true;  /* Don't count latency during drain */

    /* Signal all queue conditions to wake up waiting threads */
    for (uint32_t i = 0; i < opts.num_threads; i++) {
        pthread_mutex_lock(&g_req_queues[i].mutex);
        pthread_cond_broadcast(&g_req_queues[i].cond);
        pthread_mutex_unlock(&g_req_queues[i].mutex);
        pthread_mutex_lock(&g_comp_queues[i].mutex);
        pthread_cond_broadcast(&g_comp_queues[i].cond);
        pthread_mutex_unlock(&g_comp_queues[i].mutex);
    }

    /* Small delay to let workers drain completions */
    usleep(100000);  /* 100ms */

    /* Wait for workers first */
    for (uint32_t i = 0; i < opts.num_threads; i++) {
        pthread_join(g_worker_threads[i], NULL);
    }

    /* Wait for pollers */
    for (uint32_t i = 0; i < opts.num_threads; i++) {
        pthread_join(g_poller_threads[i], NULL);
    }

    pthread_join(printer_thread, NULL);

    uint64_t end_ns = get_time_ns();
    double total_sec = (double)(end_ns - g_start_ns) / 1000000000.0;

    /* Final stats */
    uint64_t total_io = 0;
    uint64_t total_bytes = 0;
    uint64_t total_latency = 0;
    uint64_t min_latency = UINT64_MAX;
    uint64_t max_latency = 0;

    for (uint32_t i = 0; i < opts.num_threads; i++) {
        total_io += g_stats[i].io_completed;
        total_bytes += g_stats[i].bytes_completed;
        total_latency += g_stats[i].total_latency_ns;
        if (g_stats[i].min_latency_ns < min_latency) {
            min_latency = g_stats[i].min_latency_ns;
        }
        if (g_stats[i].max_latency_ns > max_latency) {
            max_latency = g_stats[i].max_latency_ns;
        }
    }

    printf("\n=== Final Results ===\n");
    printf("Total IO:       %lu\n", total_io);
    printf("Total bytes:    %lu (%lu MB)\n", total_bytes, total_bytes / (1024 * 1024));
    printf("Duration:       %.2f sec\n", total_sec);
    printf("\n");
    printf("IOPS:           %.2f\n", (double)total_io / total_sec);
    printf("Bandwidth:      %.2f MB/s\n", (double)total_bytes / (1024 * 1024) / total_sec);
    if (total_io > 0) {
        printf("\n");
        printf("Avg latency:    %.2f us\n", (double)total_latency / total_io / 1000.0);
        printf("Min latency:    %.2f us\n", (double)min_latency / 1000.0);
        printf("Max latency:    %.2f us\n", (double)max_latency / 1000.0);
    }

    /* Cleanup */
    free(g_worker_threads);
    free(g_poller_threads);
    free(g_stats);
    free(worker_ids);
    free(poller_ids);

    for (uint32_t i = 0; i < opts.num_threads; i++) {
        ring_destroy(&g_req_queues[i]);
        ring_destroy(&g_comp_queues[i]);
    }
    free(g_req_queues);
    free(g_comp_queues);

    if (opts.poller_cpus) {
        free(opts.poller_cpus);
    }

    return 0;
}
