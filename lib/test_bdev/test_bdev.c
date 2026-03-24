/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2024
 * Standalone version - no SPDK dependency
 */

#include "test_bdev.h"

/* Global module context */
struct test_bdev_ctx g_test_bdev = {
    .buf_pool = NULL,
    .io_pool = NULL,
    .num_blocks = 0,
    .block_size = CUSTOM_BDEV_DEFAULT_BLOCK_SIZE,
    .num_worker_threads = CUSTOM_BDEV_DEFAULT_NUM_THREADS,
    .workers = NULL,
    .module_initialized = false,
};

/* Simple lock-free queue (SPSC) implementation */
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

/* Worker thread functions */
int
worker_thread_init(struct worker_context *ctx, uint32_t thread_id)
{
    ctx->thread_id = thread_id;
    ctx->queue = simple_queue_create(512);
    if (!ctx->queue) {
        fprintf(stderr, "Failed to create queue for thread %u\n", thread_id);
        return -1;
    }
    ctx->running = true;
    return 0;
}

void
worker_thread_finish(struct worker_context *ctx)
{
    if (ctx->queue) {
        simple_queue_free(ctx->queue);
        ctx->queue = NULL;
    }
}

static void
process_io_request(struct io_request *io)
{
    struct test_bdev_ctx *ctx = &g_test_bdev;

    /* Custom preprocessing */
    if (io->is_write) {
        test_bdev_preprocess_write(io);
        /* Add DIF info for write */
        set_dif_info(io);
    } else {
        test_bdev_preprocess_read(io);
    }

    /* For this custom bdev, we simulate I/O by copying from/to memory
     * In a real implementation, this would interact with actual storage
     */
    if (io->is_write) {
        /* Write: data is already in buffer */
        printf("WRITE: lba=%lu, len=%u, buf=%p\n",
               io->lba, io->len, io->buf);
    } else {
        /* Read: fill buffer with data (simulated) */
        printf("READ: lba=%lu, len=%u, buf=%p\n",
               io->lba, io->len, io->buf);
        test_bdev_postprocess_read(io);

        /* Verify DIF for read */
        verify_dif_info(io);
    }

    test_bdev_postprocess_write(io);

    /* Release rate limit */
    rate_limit_release(&ctx->rate_limit, io->len * ctx->block_size);

    /* Free buffer */
    if (io->buf) {
        free(io->buf);
    }

    /* Release io_request back to pool */
    free(io);
}

void *
worker_thread_func(void *arg)
{
    struct worker_context *ctx = (struct worker_context *)arg;
    struct io_request *io;

    printf("Worker thread %u started\n", ctx->thread_id);

    while (ctx->running) {
        /* Try to pop from queue */
        if (simple_queue_pop(ctx->queue, &io) == 0) {
            process_io_request(io);
        } else {
            /* Queue empty, yield */
            sched_yield();
        }
    }

    printf("Worker thread %u stopped\n", ctx->thread_id);
    return NULL;
}

/* Memory pool functions - using simple malloc/free */
void *
alloc_io_buffer(struct test_bdev_ctx *ctx)
{
    (void)ctx;
    void *buf = malloc(CUSTOM_BDEV_DEFAULT_BLOCK_SIZE * 64);
    return buf;
}

void
free_io_buffer(struct test_bdev_ctx *ctx, void *buf)
{
    (void)ctx;
    if (buf) {
        free(buf);
    }
}

/* Custom I/O preprocessing/Postprocessing hooks */
void
test_bdev_preprocess_read(struct io_request *io)
{
    /* Placeholder for custom read preprocessing */
    /* Example: decryption, decompression, cache lookup */
    (void)io;
}

void
test_bdev_preprocess_write(struct io_request *io)
{
    /* Placeholder for custom write preprocessing */
    /* Example: encryption, compression, cache insertion */
    (void)io;
}

void
test_bdev_postprocess_read(struct io_request *io)
{
    /* Placeholder for custom read postprocessing */
    (void)io;
}

void
test_bdev_postprocess_write(struct io_request *io)
{
    /* Placeholder for custom write postprocessing */
    (void)io;
}

/* Module initialization */
int
test_bdev_init(void)
{
    struct test_bdev_ctx *ctx = &g_test_bdev;

    printf("Custom bdev module initializing\n");

    /* Initialize rate limiting */
    rate_limit_init(&ctx->rate_limit, 0, 0); /* No limits by default */

    /* Note: We use malloc/free directly, no memory pool needed */

    /* Initialize worker threads */
    ctx->workers = calloc(ctx->num_worker_threads, sizeof(struct worker_context));
    if (!ctx->workers) {
        fprintf(stderr, "Failed to allocate worker contexts\n");
        rate_limit_finish(&ctx->rate_limit);
        return -1;
    }

    for (uint32_t i = 0; i < ctx->num_worker_threads; i++) {
        if (worker_thread_init(&ctx->workers[i], i) != 0) {
            fprintf(stderr, "Failed to initialize worker thread %u\n", i);
            /* Cleanup will happen in finish */
            return -1;
        }
    }

    ctx->module_initialized = true;

    printf("Custom bdev module initialized successfully\n");
    return 0;
}

void
test_bdev_finish(void)
{
    struct test_bdev_ctx *ctx = &g_test_bdev;

    printf("Custom bdev module finishing\n");

    if (!ctx->module_initialized) {
        return;
    }

    /* Stop worker threads */
    if (ctx->workers) {
        for (uint32_t i = 0; i < ctx->num_worker_threads; i++) {
            ctx->workers[i].running = false;
            worker_thread_finish(&ctx->workers[i]);
        }
        free(ctx->workers);
        ctx->workers = NULL;
    }

    /* Cleanup rate limiting */
    rate_limit_finish(&ctx->rate_limit);

    ctx->module_initialized = false;

    printf("Custom bdev module finished\n");
}
