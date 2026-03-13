/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2024
 */

#include "test_bdev.h"
#include <spdk/log.h>
#include <spdk/string.h>

SPDK_LOG_REGISTER_COMPONENT("test_bdev", SPDK_LOG_CUSTOM_BDEV)

/* Global module context */
struct test_bdev_ctx g_test_bdev = {
    .bdev = NULL,
    .desc = NULL,
    .buf_pool = NULL,
    .num_blocks = 0,
    .block_size = CUSTOM_BDEV_DEFAULT_BLOCK_SIZE,
    .num_worker_threads = CUSTOM_BDEV_DEFAULT_NUM_THREADS,
    .workers = NULL,
    .module_initialized = false,
};

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
        SPDK_ERRLOG("DIF verification failed: expected 0x%04x, got 0x%04x\n",
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

    SPDK_NOTICELOG("Rate limit initialized: max_iops=%lu, max_bw_mb=%lu MB/s\n",
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
    ctx->queue = spdk_spsc_fifo_create(512, sizeof(struct io_request *));
    if (!ctx->queue) {
        SPDK_ERRLOG("Failed to create SPSC queue for thread %u\n", thread_id);
        return -1;
    }
    ctx->running = true;
    return 0;
}

void
worker_thread_finish(struct worker_context *ctx)
{
    if (ctx->queue) {
        spdk_spsc_fifo_free(ctx->queue);
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
        /* Write: data is already in buffer from memory pool */
        SPDK_DEBUGLOG(test_bdev, "WRITE: lba=%lu, len=%u, buf=%p\n",
                      io->lba, io->len, io->buf);
    } else {
        /* Read: fill buffer with data (simulated) */
        SPDK_DEBUGLOG(test_bdev, "READ: lba=%lu, len=%u, buf=%p\n",
                      io->lba, io->len, io->buf);
        test_bdev_postprocess_read(io);

        /* Verify DIF for read */
        verify_dif_info(io);
    }

    test_bdev_postprocess_write(io);

    /* Release rate limit */
    rate_limit_release(&ctx->rate_limit, io->len * ctx->block_size);

    /* Free buffer back to pool */
    free_io_buffer(ctx, io->buf);

    /* Complete the I/O */
    spdk_bdev_io_complete(io->bio, SPDK_BDEV_IO_STATUS_SUCCESS);
}

void *
worker_thread_func(void *arg)
{
    struct worker_context *ctx = (struct worker_context *)arg;
    struct io_request *io;

    SPDK_NOTICELOG("Worker thread %u started\n", ctx->thread_id);

    while (ctx->running) {
        /* Try to pop from queue */
        if (spdk_spsc_fifo_pop(ctx->queue, (void **)&io) == 0) {
            process_io_request(io);
        } else {
            /* Queue empty, yield */
            sched_yield();
        }
    }

    SPDK_NOTICELOG("Worker thread %u stopped\n", ctx->thread_id);
    return NULL;
}

/* Memory pool functions */
void *
alloc_io_buffer(struct test_bdev_ctx *ctx)
{
    void *buf;
    int ret;

    ret = spdk_mempool_get(ctx->buf_pool, &buf);
    if (ret != 0) {
        SPDK_ERRLOG("Failed to get buffer from pool: %s\n", spdk_strerror(-ret));
        return NULL;
    }

    return buf;
}

void
free_io_buffer(struct test_bdev_ctx *ctx, void *buf)
{
    if (buf) {
        spdk_mempool_put(ctx->buf_pool, buf);
    }
}

/* Custom I/O preprocessing/Postprocessing hooks */
void
test_bdev_preprocess_read(struct io_request *io)
{
    /* Placeholder for custom read preprocessing */
    /* Example: decryption, decompression, cache lookup */
}

void
test_bdev_preprocess_write(struct io_request *io)
{
    /* Placeholder for custom write preprocessing */
    /* Example: encryption, compression, cache insertion */
}

void
test_bdev_postprocess_read(struct io_request *io)
{
    /* Placeholder for custom read postprocessing */
}

void
test_bdev_postprocess_write(struct io_request *io)
{
    /* Placeholder for custom write postprocessing */
}

/* SPDK Bdev module implementation */
static int
test_bdev_init(void)
{
    struct test_bdev_ctx *ctx = &g_test_bdev;

    SPDK_NOTICELOG("Custom bdev module initializing\n");

    /* Initialize rate limiting */
    rate_limit_init(&ctx->rate_limit, 0, 0); /* No limits by default */

    /* Create memory pool for I/O buffers (500MB) */
    ctx->buf_pool = spdk_mempool_create("test_bdev_pool",
                                          1024, /* 1024 buffers */
                                          CUSTOM_BDEV_DEFAULT_BLOCK_SIZE * 64, /* 64 blocks per buffer */
                                          32); /* Cache size */
    if (!ctx->buf_pool) {
        SPDK_ERRLOG("Failed to create memory pool\n");
        rate_limit_finish(&ctx->rate_limit);
        return -1;
    }

    SPDK_NOTICELOG("Created 512MB memory pool\n");

    /* Initialize worker threads */
    ctx->workers = calloc(ctx->num_worker_threads, sizeof(struct worker_context));
    if (!ctx->workers) {
        SPDK_ERRLOG("Failed to allocate worker contexts\n");
        spdk_mempool_free(ctx->buf_pool);
        rate_limit_finish(&ctx->rate_limit);
        return -1;
    }

    for (uint32_t i = 0; i < ctx->num_worker_threads; i++) {
        if (worker_thread_init(&ctx->workers[i], i) != 0) {
            SPDK_ERRLOG("Failed to initialize worker thread %u\n", i);
            /* Cleanup will happen in finish */
            return -1;
        }
    }

    ctx->module_initialized = true;

    SPDK_NOTICELOG("Custom bdev module initialized successfully\n");
    return 0;
}

static void
test_bdev_finish(void)
{
    struct test_bdev_ctx *ctx = &g_test_bdev;

    SPDK_NOTICELOG("Custom bdev module finishing\n");

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

    /* Free memory pool */
    if (ctx->buf_pool) {
        spdk_mempool_free(ctx->buf_pool);
        ctx->buf_pool = NULL;
    }

    /* Cleanup rate limiting */
    rate_limit_finish(&ctx->rate_limit);

    ctx->module_initialized = false;

    SPDK_NOTICELOG("Custom bdev module finished\n");
}

/* Bdev function table */
static int
test_bdev_create(struct spdk_bdev **bdev, struct spdk_bdev_desc *desc,
                   const char *bdev_name, uint64_t num_blocks,
                   uint32_t block_size)
{
    struct test_bdev_ctx *ctx = &g_test_bdev;
    struct spdk_bdev *b = NULL;
    int ret;

    SPDK_NOTICELOG("Creating custom bdev: %s, blocks=%lu, block_size=%u\n",
                   bdev_name, num_blocks, block_size);

    b = spdk_bdev_create_ext(bdev_name, NULL, num_blocks, block_size);
    if (!b) {
        SPDK_ERRLOG("Failed to create bdev: %s\n", bdev_name);
        return -1;
    }

    /* Set up channel (thread) information */
    ret = spdk_bdev_set_thread(b, spdk_get_thread());
    if (ret != 0) {
        SPDK_ERRLOG("Failed to set thread for bdev: %d\n", ret);
        spdk_bdev_destroy(b);
        return ret;
    }

    ctx->bdev = b;
    ctx->desc = desc;
    ctx->num_blocks = num_blocks;
    ctx->block_size = block_size;

    *bdev = b;
    return 0;
}

static int
test_bdev_delete(struct spdk_bdev *bdev)
{
    if (bdev) {
        spdk_bdev_destroy(bdev);
    }
    return 0;
}

static void
test_bdev_read(struct spdk_bdev_io *bio)
{
    struct test_bdev_ctx *ctx = &g_test_bdev;
    struct io_request *io;
    void *buf;
    uint64_t lba = spdk_bdev_io_get_offset(bio);
    uint32_t len = spdk_bdev_io_get_num_blocks(bio);
    uint32_t thread_id;

    /* Check rate limit */
    if (!rate_limit_check_and_acquire(&ctx->rate_limit)) {
        /* Would need to queue the request - for now, return busy */
        spdk_bdev_io_complete(bio, SPDK_BDEV_IO_STATUS_FAILED);
        return;
    }

    /* Allocate buffer from pool */
    buf = alloc_io_buffer(ctx);
    if (!buf) {
        rate_limit_release(&ctx->rate_limit, 0);
        spdk_bdev_io_complete(bio, SPDK_BDEV_IO_STATUS_NOMEM);
        return;
    }

    /* Create I/O request */
    io = calloc(1, sizeof(struct io_request));
    if (!io) {
        free_io_buffer(ctx, buf);
        rate_limit_release(&ctx->rate_limit, 0);
        spdk_bdev_io_complete(bio, SPDK_BDEV_IO_STATUS_NOMEM);
        return;
    }

    io->bio = bio;
    io->buf = buf;
    io->lba = lba;
    io->len = len;
    io->is_write = false;

    /* Dispatch to worker thread based on LBA */
    thread_id = lba_to_thread(lba, ctx->num_worker_threads);
    io->thread_id = thread_id;

    /* Push to worker queue */
    if (spdk_spsc_fifo_push(ctx->workers[thread_id].queue, io) != 0) {
        free(io);
        free_io_buffer(ctx, buf);
        rate_limit_release(&ctx->rate_limit, 0);
        spdk_bdev_io_complete(bio, SPDK_BDEV_IO_STATUS_FAILED);
        return;
    }
}

static void
test_bdev_write(struct spdk_bdev_io *bio)
{
    struct test_bdev_ctx *ctx = &g_test_bdev;
    struct io_request *io;
    void *buf;
    uint64_t lba = spdk_bdev_io_get_offset(bio);
    uint32_t len = spdk_bdev_io_get_num_blocks(bio);
    uint32_t thread_id;

    /* Check rate limit */
    if (!rate_limit_check_and_acquire(&ctx->rate_limit)) {
        spdk_bdev_io_complete(bio, SPDK_BDEV_IO_STATUS_FAILED);
        return;
    }

    /* Allocate buffer from pool */
    buf = alloc_io_buffer(ctx);
    if (!buf) {
        rate_limit_release(&ctx->rate_limit, 0);
        spdk_bdev_io_complete(bio, SPDK_BDEV_IO_STATUS_NOMEM);
        return;
    }

    /* Copy data from user buffer */
    void *src_buf = spdk_bdev_io_get_buf(bio);
    memcpy(buf, src_buf, len * ctx->block_size);

    /* Create I/O request */
    io = calloc(1, sizeof(struct io_request));
    if (!io) {
        free_io_buffer(ctx, buf);
        rate_limit_release(&ctx->rate_limit, 0);
        spdk_bdev_io_complete(bio, SPDK_BDEV_IO_STATUS_NOMEM);
        return;
    }

    io->bio = bio;
    io->buf = buf;
    io->lba = lba;
    io->len = len;
    io->is_write = true;

    /* Dispatch to worker thread based on LBA */
    thread_id = lba_to_thread(lba, ctx->num_worker_threads);
    io->thread_id = thread_id;

    /* Push to worker queue */
    if (spdk_spsc_fifo_push(ctx->workers[thread_id].queue, io) != 0) {
        free(io);
        free_io_buffer(ctx, buf);
        rate_limit_release(&ctx->rate_limit, 0);
        spdk_bdev_io_complete(bio, SPDK_BDEV_IO_STATUS_FAILED);
        return;
    }
}

/* Module definition */
static struct spdk_bdev_fn_table test_bdev_fn_table = {
    .destructor = NULL,
    .submit_request = NULL, /* Will be set per bdev */
    .get_device_info = NULL,
    .get_io_channel = NULL,
};

/* Note: This is a simplified implementation. In real SPDK bdev module,
 * you would register the module with SPDK framework and implement
 * the full bdev layer interface.
 */

/* Register the module */
SPDK_BDEV_MODULE_REGISTER(test_bdev, NULL)
