/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2024
 */

#ifndef CUSTOM_BDEV_H
#define CUSTOM_BDEV_H

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>
#include <sys/queue.h>

#include <spdk/bdev.h>
#include <spdk/bdev_module.h>
#include <spdk/mempool.h>
#include <spdk/spsc_fifo.h>
#include <spdk/thread.h>

/* Configuration */
#define CUSTOM_BDEV_DEFAULT_SIZE_MB     512
#define CUSTOM_BDEV_DEFAULT_BLOCK_SIZE  512
#define CUSTOM_BDEV_DEFAULT_NUM_THREADS 4
#define CUSTOM_BDEV_MAX_QUEUE_DEPTH     128

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
    struct spdk_bdev_io *bio;
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
    /* Basic I/O info */
    struct spdk_bdev_io *bio;      /* SPDK bdev I/O */
    void *buf;                      /* Buffer from memory pool */
    uint64_t lba;                   /* Logical block address */
    uint32_t len;                   /* Length in blocks */
    uint32_t thread_id;             /* Target thread ID */
    struct dif_info dif;            /* DIF information */
    bool is_write;                  /* Write operation flag */

    /* 128 fields for custom data */
    uint64_t field[128];
};

/* Worker thread context */
struct worker_context {
    uint32_t thread_id;
    struct spdk_thread *spdk_thread;
    struct spdk_spsc_fifo *queue;
    bool running;
};

/* Custom bdev context */
struct test_bdev_ctx {
    struct spdk_bdev *bdev;
    struct spdk_bdev_desc *desc;
    struct spdk_mempool *buf_pool;      /* Buffer memory pool */
    struct spdk_mempool *io_pool;       /* I/O request memory pool */
    struct rate_limit rate_limit;
    uint64_t num_blocks;
    uint32_t block_size;
    uint32_t num_worker_threads;
    struct worker_context *workers;
    bool module_initialized;
};

/* Global module context */
extern struct test_bdev_ctx g_test_bdev;

/* Rate limit functions */
int rate_limit_init(struct rate_limit *limit, uint64_t max_iops, uint64_t max_bw_mb);
void rate_limit_finish(struct rate_limit *limit);
bool rate_limit_check_and_acquire(struct rate_limit *limit);
void rate_limit_release(struct rate_limit *limit, uint32_t bytes);

/* Thread dispatch functions */
uint32_t lba_to_thread(uint64_t lba, uint32_t num_threads);
int worker_thread_init(struct worker_context *ctx, uint32_t thread_id);
void worker_thread_finish(struct worker_context *ctx);
void *worker_thread_func(void *arg);

/* DIF functions */
uint16_t calc_guard_tag(void *data, uint32_t len);
void set_dif_info(struct io_request *io);
void verify_dif_info(struct io_request *io);

/* Memory pool functions */
void *alloc_io_buffer(struct test_bdev_ctx *ctx);
void free_io_buffer(struct test_bdev_ctx *ctx, void *buf);

/* Custom I/O logic (can be customized) */
void test_bdev_preprocess_read(struct io_request *io);
void test_bdev_preprocess_write(struct io_request *io);
void test_bdev_postprocess_read(struct io_request *io);
void test_bdev_postprocess_write(struct io_request *io);

#endif /* CUSTOM_BDEV_H */
