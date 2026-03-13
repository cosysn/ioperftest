/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2024
 * Standalone test version - no SPDK dependency
 */

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>
#include <sys/queue.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sched.h>
#include <signal.h>
#include <time.h>
#include <errno.h>

/* Simplified structures without SPDK dependencies */

/* Rate limit structure */
struct rate_limit {
    uint64_t max_iops;          /* Max IOPS (0 = unlimited) */
    uint64_t max_bw_mb;         /* Max bandwidth MB/s (0 = unlimited) */
    _Atomic uint64_t cur_iops;  /* Current IOPS counter */
    _Atomic uint64_t cur_bytes; /* Current bandwidth counter */
    pthread_mutex_t queue_lock; /* Queue lock */
    TAILQ_HEAD(, pending_io) pending_queue; /* Pending I/O queue */
};

/* Pending I/O entry */
struct pending_io {
    void *data;
    TAILQ_ENTRY(pending_io) link;
};

/* DIF information structure */
struct dif_info {
    uint16_t guard_tag;     /* CRC checksum */
    uint32_t ref_tag;       /* Reference tag (based on LBA) */
    uint16_t app_tag;       /* Application tag */
};

/* I/O request structure */
struct io_request {
    void *buf;                      /* Buffer */
    uint64_t lba;                   /* Logical block address */
    uint32_t len;                   /* Length in blocks */
    uint32_t thread_id;             /* Target thread ID */
    struct dif_info dif;            /* DIF information */
    bool is_write;                  /* Write operation flag */
};

/* Simple lock-free queue (SPSC) implementation */
struct simple_queue {
    volatile uint64_t write_idx;
    volatile uint64_t read_idx;
    struct io_request **entries;
    uint64_t size;
};

struct simple_queue *
simple_queue_create(uint64_t size)
{
    struct simple_queue *q = calloc(1, sizeof(struct simple_queue));
    if (!q) return NULL;

    q->entries = calloc(size, sizeof(struct io_request *));
    if (!q->entries) {
        free(q);
        return NULL;
    }

    q->size = size;
    q->write_idx = 0;
    q->read_idx = 0;
    return q;
}

void
simple_queue_free(struct simple_queue *q)
{
    if (q) {
        free(q->entries);
        free(q);
    }
}

int
simple_queue_push(struct simple_queue *q, struct io_request *io)
{
    uint64_t next_write = (q->write_idx + 1) % q->size;
    if (next_write == q->read_idx) {
        return -1; /* Queue full */
    }
    q->entries[q->write_idx] = io;
    q->write_idx = next_write;
    return 0;
}

int
simple_queue_pop(struct simple_queue *q, struct io_request **io)
{
    if (q->read_idx == q->write_idx) {
        return -1; /* Queue empty */
    }
    *io = q->entries[q->read_idx];
    q->read_idx = (q->read_idx + 1) % q->size;
    return 0;
}

/* CRC16 table for DIF guard tag calculation */
static uint16_t crc16_table[256];
static bool crc16_initialized = false;

static void
init_crc16(void)
{
    uint16_t poly = 0x1021;
    for (uint32_t i = 0; i < 256; i++) {
        uint16_t val = 0;
        uint16_t tmp = i << 8;
        for (uint32_t j = 0; j < 8; j++) {
            if ((val ^ tmp) & 0x8000) {
                val = (val << 1) ^ poly;
            } else {
                val = val << 1;
            }
            tmp = tmp << 1;
        }
        crc16_table[i] = val;
    }
    crc16_initialized = true;
}

uint16_t
calc_guard_tag(void *data, uint32_t len)
{
    uint16_t crc = 0;
    uint8_t *ptr = (uint8_t *)data;

    if (!crc16_initialized) {
        init_crc16();
    }

    for (uint32_t i = 0; i < len; i++) {
        uint8_t index = (crc >> 8) ^ ptr[i];
        crc = (crc << 8) ^ crc16_table[index];
    }

    return crc;
}

void
set_dif_info(struct io_request *io)
{
    io->dif.guard_tag = calc_guard_tag(io->buf, io->len * 512);
    io->dif.ref_tag = (uint32_t)(io->lba & 0xFFFFFFFF);
    io->dif.app_tag = 0xFFFF;
}

void
verify_dif_info(struct io_request *io)
{
    uint16_t expected_guard = calc_guard_tag(io->buf, io->len * 512);
    if (expected_guard != io->dif.guard_tag) {
        fprintf(stderr, "DIF verification failed: expected 0x%04x, got 0x%04x\n",
                expected_guard, io->dif.guard_tag);
    }
}

/* Rate limit implementation */
int
rate_limit_init(struct rate_limit *limit, uint64_t max_iops, uint64_t max_bw_mb)
{
    limit->max_iops = max_iops;
    limit->max_bw_mb = max_bw_mb;
    atomic_init(&limit->cur_iops, 0);
    atomic_init(&limit->cur_bytes, 0);
    pthread_mutex_init(&limit->queue_lock, NULL);
    TAILQ_INIT(&limit->pending_queue);

    printf("Rate limit initialized: max_iops=%lu, max_bw_mb=%lu MB/s\n",
           max_iops, max_bw_mb);

    return 0;
}

void
rate_limit_finish(struct rate_limit *limit)
{
    pthread_mutex_destroy(&limit->queue_lock);
}

bool
rate_limit_check_and_acquire(struct rate_limit *limit)
{
    /* If no limits set, allow immediately */
    if (limit->max_iops == 0 && limit->max_bw_mb == 0) {
        return true;
    }

    pthread_mutex_lock(&limit->queue_lock);

    uint64_t cur_iops = atomic_load(&limit->cur_iops);
    uint64_t cur_bytes = atomic_load(&limit->cur_bytes);

    /* Check IOPS limit */
    if (limit->max_iops > 0 && cur_iops >= limit->max_iops) {
        pthread_mutex_unlock(&limit->queue_lock);
        return false;
    }

    /* Check bandwidth limit (1 MB = 1024 * 1024 bytes) */
    if (limit->max_bw_mb > 0 && cur_bytes >= (limit->max_bw_mb * 1024 * 1024)) {
        pthread_mutex_unlock(&limit->queue_lock);
        return false;
    }

    /* Acquire the slot */
    atomic_fetch_add(&limit->cur_iops, 1);

    pthread_mutex_unlock(&limit->queue_lock);
    return true;
}

void
rate_limit_release(struct rate_limit *limit, uint32_t bytes)
{
    if (limit->max_iops > 0 || limit->max_bw_mb > 0) {
        atomic_fetch_sub(&limit->cur_iops, 1);
        atomic_fetch_sub(&limit->cur_bytes, bytes);
    }
}

/* Thread dispatch - LBA to thread mapping */
uint32_t
lba_to_thread(uint64_t lba, uint32_t num_threads)
{
    /* Use XOR hash for better distribution */
    return ((lba >> 12) ^ (lba >> 24)) % num_threads;
}

/* Worker thread context */
struct worker_context {
    uint32_t thread_id;
    pthread_t thread;
    struct simple_queue *queue;
    bool running;
    struct rate_limit *rate_limit;
    void *buf_pool;
    size_t buf_size;
};

void *
worker_thread_func(void *arg)
{
    struct worker_context *ctx = (struct worker_context *)arg;
    struct io_request *io;

    printf("Worker thread %u started\n", ctx->thread_id);

    while (ctx->running) {
        /* Try to pop from queue */
        if (simple_queue_pop(ctx->queue, &io) == 0) {
            /* Process I/O request */
            if (io->is_write) {
                /* Add DIF info for write */
                set_dif_info(io);
                /* Simulate write: save data and DIF to "storage"
                 * In this test, we just keep the buffer as-is
                 * The DIF will be verified on read
                 */
            } else {
                /* For read: in a real system, we'd read the stored DIF
                 * For this test, we skip verification as it was never stored
                 */
                /* verify_dif_info(io); */
            }

            /* Release rate limit */
            rate_limit_release(ctx->rate_limit, io->len * 512);

            /* Free buffer back to pool */
            if (io->buf) {
                free(io->buf);
            }
            free(io);
        } else {
            /* Queue empty, yield */
            sched_yield();
        }
    }

    printf("Worker thread %u stopped\n", ctx->thread_id);
    return NULL;
}

int
worker_thread_init(struct worker_context *ctx, uint32_t thread_id,
                   struct rate_limit *rate_limit, uint32_t queue_size)
{
    ctx->thread_id = thread_id;
    ctx->queue = simple_queue_create(queue_size);
    if (!ctx->queue) {
        return -1;
    }
    ctx->running = true;
    ctx->rate_limit = rate_limit;
    ctx->buf_size = 64 * 512; /* 64 blocks */

    if (pthread_create(&ctx->thread, NULL, worker_thread_func, ctx) != 0) {
        simple_queue_free(ctx->queue);
        return -1;
    }

    return 0;
}

void
worker_thread_finish(struct worker_context *ctx)
{
    ctx->running = false;
    if (ctx->thread) {
        pthread_join(ctx->thread, NULL);
    }
    if (ctx->queue) {
        simple_queue_free(ctx->queue);
    }
}

/* Main test application */
#define NUM_THREADS 2
#define QUEUE_SIZE 256

static volatile bool g_running = true;

static void
signal_handler(int signum)
{
    (void)signum;
    g_running = false;
}

int main(int argc, char *argv[])
{
    struct rate_limit rate_limit = {0};
    struct worker_context workers[NUM_THREADS];
    uint64_t max_iops = 0;
    uint64_t max_bw_mb = 0;

    printf("Custom Bdev Test Application (Standalone)\n");
    printf("===========================================\n\n");

    /* Parse arguments */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--max-iops") == 0 && i + 1 < argc) {
            max_iops = atoll(argv[++i]);
        } else if (strcmp(argv[i], "--max-bw") == 0 && i + 1 < argc) {
            max_bw_mb = atoll(argv[++i]);
        } else if (strcmp(argv[i], "--help") == 0) {
            printf("Usage: %s [options]\n", argv[0]);
            printf("  --max-iops=N    Set maximum IOPS (0 = unlimited)\n");
            printf("  --max-bw=N     Set maximum bandwidth in MB/s (0 = unlimited)\n");
            printf("  --help         Show this help\n");
            return 0;
        }
    }

    /* Initialize rate limit with user-provided values */
    rate_limit_init(&rate_limit, max_iops, max_bw_mb);

    /* Initialize worker threads */
    for (uint32_t i = 0; i < NUM_THREADS; i++) {
        if (worker_thread_init(&workers[i], i, &rate_limit, QUEUE_SIZE) != 0) {
            fprintf(stderr, "Failed to initialize worker thread %u\n", i);
            return 1;
        }
    }

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    printf("\nRunning with %u worker threads\n", NUM_THREADS);
    printf("Rate limit: max_iops=%lu, max_bw_mb=%lu MB/s\n", max_iops, max_bw_mb);
    printf("Press Ctrl+C to exit\n\n");

    /* Generate test I/O requests */
    uint64_t total_io = 0;
    uint64_t last_report = 0;
    struct timespec start_time, current_time;

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    while (g_running) {
        /* Submit test I/O requests */
        for (int i = 0; i < 10 && g_running; i++) {
            /* Check rate limit */
            if (!rate_limit_check_and_acquire(&rate_limit)) {
                /* Rate limited, wait a bit */
                usleep(1000);
                continue;
            }

            /* Create I/O request */
            struct io_request *io = calloc(1, sizeof(struct io_request));
            if (!io) {
                rate_limit_release(&rate_limit, 0);
                continue;
            }

            io->buf = malloc(64 * 512);
            if (!io->buf) {
                free(io);
                rate_limit_release(&rate_limit, 0);
                continue;
            }

            /* Fill buffer with test data */
            memset(io->buf, (int)(total_io & 0xFF), 64 * 512);

            io->lba = total_io % 1000000;
            io->len = 64;
            io->is_write = (total_io % 2 == 0);

            /* Dispatch to worker thread based on LBA */
            uint32_t thread_id = lba_to_thread(io->lba, NUM_THREADS);

            /* Push to worker queue */
            if (simple_queue_push(workers[thread_id].queue, io) != 0) {
                free(io->buf);
                free(io);
                rate_limit_release(&rate_limit, 0);
            } else {
                total_io++;
            }
        }

        /* Report statistics every second */
        clock_gettime(CLOCK_MONOTONIC, &current_time);
        uint64_t elapsed = (current_time.tv_sec - start_time.tv_sec);
        if (elapsed > last_report) {
            last_report = elapsed;
            uint64_t cur_iops = atomic_load(&rate_limit.cur_iops);
            printf("\rElapsed: %lus, Total I/O: %lu, Current IOPS: %lu",
                   elapsed, total_io, cur_iops);
            fflush(stdout);
        }

        usleep(100);
    }

    printf("\n\nShutting down...\n");

    /* Wait for pending I/O to complete */
    sleep(1);

    /* Stop worker threads */
    for (uint32_t i = 0; i < NUM_THREADS; i++) {
        worker_thread_finish(&workers[i]);
    }

    rate_limit_finish(&rate_limit);

    printf("Test complete! Total I/O: %lu\n", total_io);

    return 0;
}
