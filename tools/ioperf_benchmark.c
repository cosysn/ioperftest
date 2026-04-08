/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2024
 * IOperf async benchmark tool
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

#include "ioperf.h"

/* Default configuration */
#define DEFAULT_DISK_SIZE_MB   64
#define DEFAULT_BLOCK_SIZE     512
#define DEFAULT_NUM_THREADS    4
#define DEFAULT_IO_DEPTH       128
#define DEFAULT_TEST_DURATION_SEC 10

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
    uint32_t *poller_cpus;       /* NEW: CPU IDs for poller affinity */
    uint32_t  poller_cpus_count; /* NEW: length of poller_cpus array */
};

/* Per-thread statistics */
struct thread_stats {
    uint64_t io_completed;
    uint64_t bytes_completed;
    uint64_t total_latency_ns;
    uint64_t min_latency_ns;
    uint64_t max_latency_ns;
};

/* IO wrapper for async operation */
struct io_wrapper {
    struct ioperf_io_ctx ctx;
    uint64_t submit_ns;
    struct thread_stats *stats;
    struct io_tracker *tracker;
};

/* Track in-flight IO */
struct io_tracker {
    _Atomic uint32_t in_flight;
    uint32_t max_in_flight;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
};

/* Global state */
static struct ioperf_disk *g_disk;
static volatile bool g_running;
static pthread_t *g_worker_threads;
static struct thread_stats *g_stats;
static uint32_t g_num_threads;
static uint64_t g_start_ns;
static struct io_tracker g_tracker;

/* Get current time in nanoseconds */
static inline uint64_t
get_time_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

/* IO completion callback */
static void
io_complete(void *arg)
{
    struct io_wrapper *w = (struct io_wrapper *)arg;
    uint64_t complete_ns = get_time_ns();
    uint64_t latency = complete_ns - w->submit_ns;

    w->stats->io_completed++;
    w->stats->bytes_completed += w->ctx.io_size;
    w->stats->total_latency_ns += latency;
    if (w->stats->min_latency_ns == 0 || latency < w->stats->min_latency_ns) {
        w->stats->min_latency_ns = latency;
    }
    if (latency > w->stats->max_latency_ns) {
        w->stats->max_latency_ns = latency;
    }

    uint32_t prev = atomic_fetch_sub(&g_tracker.in_flight, 1);
    if (prev == g_tracker.max_in_flight) {
        /* Was full, now available - signal waiters */
        pthread_mutex_lock(&g_tracker.mutex);
        pthread_cond_signal(&g_tracker.cond);
        pthread_mutex_unlock(&g_tracker.mutex);
    }
}

/* Sequential write test - async */
static void *
seq_write_worker(void *arg)
{
    struct thread_stats *stats = (struct thread_stats *)arg;
    struct io_wrapper *ios;
    uint32_t idx = 0;
    uint64_t offset = 0;
    uint32_t io_depth = DEFAULT_IO_DEPTH;

    /* Pre-allocate IO wrappers */
    ios = calloc(io_depth, sizeof(struct io_wrapper));
    if (!ios) return NULL;

    while (g_running) {
        /* Submit batch of IOs up to depth */
        uint32_t batch_size = 0;
        while (batch_size < io_depth && atomic_load(&g_tracker.in_flight) < g_tracker.max_in_flight) {
            struct io_wrapper *w = &ios[idx % io_depth];

            ioperf_io_ctx_init(&w->ctx);
            w->ctx.type = IOPERF_IO_WRITE;
            w->ctx.offset_blocks = offset;
            w->ctx.num_blocks = 1;
            w->ctx.io_size = g_disk->block_size;
            w->ctx.complete = io_complete;
            w->ctx.complete_arg = w;
            w->stats = stats;
            w->submit_ns = get_time_ns();

            int rc = ioperf_submit_io(g_disk, &w->ctx);
            if (rc == 0) {
                atomic_fetch_add(&g_tracker.in_flight, 1);
                batch_size++;
                idx++;
            }

            offset += g_disk->num_threads * g_disk->block_size;
            if (offset >= g_disk->block_count * g_disk->block_size) {
                offset = 0;
            }
        }
    }

    /* Wait for remaining IO */
    while (atomic_load(&g_tracker.in_flight) > 0) {
        /* spin wait */
    }

    free(ios);
    return NULL;
}

/* Sequential read test - async */
static void *
seq_read_worker(void *arg)
{
    struct thread_stats *stats = (struct thread_stats *)arg;
    struct io_wrapper *ios;
    uint32_t idx = 0;
    uint64_t offset = 0;
    uint32_t io_depth = DEFAULT_IO_DEPTH;

    ios = calloc(io_depth, sizeof(struct io_wrapper));
    if (!ios) return NULL;

    while (g_running) {
        uint32_t batch_size = 0;
        while (batch_size < io_depth && atomic_load(&g_tracker.in_flight) < g_tracker.max_in_flight) {
            struct io_wrapper *w = &ios[idx % io_depth];

            ioperf_io_ctx_init(&w->ctx);
            w->ctx.type = IOPERF_IO_READ;
            w->ctx.offset_blocks = offset;
            w->ctx.num_blocks = 1;
            w->ctx.io_size = g_disk->block_size;
            w->ctx.complete = io_complete;
            w->ctx.complete_arg = w;
            w->stats = stats;
            w->submit_ns = get_time_ns();

            int rc = ioperf_submit_io(g_disk, &w->ctx);
            if (rc == 0) {
                atomic_fetch_add(&g_tracker.in_flight, 1);
                batch_size++;
                idx++;
            }

            offset += g_disk->num_threads * g_disk->block_size;
            if (offset >= g_disk->block_count * g_disk->block_size) {
                offset = 0;
            }
        }
    }

    while (atomic_load(&g_tracker.in_flight) > 0) {
        /* spin wait */
    }

    free(ios);
    return NULL;
}

/* Random I/O test - async */
static void *
rand_io_worker(void *arg)
{
    struct thread_stats *stats = (struct thread_stats *)arg;
    struct io_wrapper *ios;
    uint32_t idx = 0;
    uint32_t io_depth = DEFAULT_IO_DEPTH;

    ios = calloc(io_depth, sizeof(struct io_wrapper));
    if (!ios) return NULL;

    while (g_running) {
        uint32_t batch_size = 0;
        while (batch_size < io_depth && atomic_load(&g_tracker.in_flight) < g_tracker.max_in_flight) {
            struct io_wrapper *w = &ios[idx % io_depth];

            ioperf_io_ctx_init(&w->ctx);
            w->ctx.type = (rand() % 2 == 0) ? IOPERF_IO_READ : IOPERF_IO_WRITE;
            w->ctx.offset_blocks = (uint64_t)(rand() % (g_disk->block_count - 1));
            w->ctx.num_blocks = 1;
            w->ctx.io_size = g_disk->block_size;
            w->ctx.complete = io_complete;
            w->ctx.complete_arg = w;
            w->stats = stats;
            w->submit_ns = get_time_ns();

            int rc = ioperf_submit_io(g_disk, &w->ctx);
            if (rc == 0) {
                atomic_fetch_add(&g_tracker.in_flight, 1);
                batch_size++;
                idx++;
            }
        }
    }

    while (atomic_load(&g_tracker.in_flight) > 0) {
        /* spin wait */
    }

    free(ios);
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

    int rc = ioperf_disk_create(&g_disk, &disk_opts);
    if (rc != 0) {
        fprintf(stderr, "Failed to create disk: %d\n", rc);
        return 1;
    }

    printf("\nDisk created successfully\n");

    /* Init tracker */
    atomic_init(&g_tracker.in_flight, 0);
    g_tracker.max_in_flight = opts.num_threads * opts.io_depth;

    /* Allocate stats */
    g_stats = calloc(opts.num_threads, sizeof(struct thread_stats));
    g_num_threads = opts.num_threads;
    g_running = true;

    void *(*worker_fn)(void *) = opts.is_rand_test ? rand_io_worker :
                                 opts.is_write_test ? seq_write_worker :
                                 seq_read_worker;

    printf("\nStarting %s test...\n\n",
           opts.is_rand_test ? "random" : opts.is_write_test ? "seq_write" : "seq_read");

    g_start_ns = get_time_ns();

    /* Create stats printer thread */
    pthread_t printer_thread;
    pthread_create(&printer_thread, NULL, stats_printer, &opts.duration_sec);

    /* Create worker threads */
    g_worker_threads = calloc(opts.num_threads, sizeof(pthread_t));
    for (uint32_t i = 0; i < opts.num_threads; i++) {
        pthread_create(&g_worker_threads[i], NULL, worker_fn, &g_stats[i]);
    }

    /* Wait for duration */
    sleep(opts.duration_sec);
    g_running = false;

    /* Wait for threads */
    for (uint32_t i = 0; i < opts.num_threads; i++) {
        pthread_join(g_worker_threads[i], NULL);
    }
    pthread_join(printer_thread, NULL);

    uint64_t end_ns = get_time_ns();
    double total_sec = (double)(end_ns - g_start_ns) / 1000000000.0;

    /* Final stats */
    uint64_t total_io = 0;
    uint64_t total_bytes = 0;
    uint64_t total_latency = 0;
    uint64_t min_latency = 0;
    uint64_t max_latency = 0;

    for (uint32_t i = 0; i < opts.num_threads; i++) {
        total_io += g_stats[i].io_completed;
        total_bytes += g_stats[i].bytes_completed;
        total_latency += g_stats[i].total_latency_ns;
        if (min_latency == 0 || g_stats[i].min_latency_ns < min_latency) {
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

    free(g_worker_threads);
    free(g_stats);
    ioperf_disk_destroy(g_disk);
    if (opts.poller_cpus) {
        free(opts.poller_cpus);
    }

    return 0;
}
