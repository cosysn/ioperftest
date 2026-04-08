/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2024
 */

#include "ioperf.h"
#include <stdio.h>
#include <errno.h>
#include <sched.h>

#define MAX_QUEUED_IO 1024

/* Forward declarations */
static void fill_all_fields(struct ioperf_io_ctx *ctx, uint64_t io_size);
static bool rate_limit_check(struct ioperf_channel *ch, struct ioperf_disk *disk, uint64_t io_size);
static void ioperf_process_io(struct ioperf_io_ctx *io_ctx);
static void *ioperf_wait_poll_thread(void *arg);
static int ioperf_hash_map_init(struct ioperf_hash_map *hash_map, size_t size);
static void ioperf_hash_map_destroy(struct ioperf_hash_map *hash_map);
static uint32_t ioperf_hash_lba(uint64_t lba, uint32_t num_threads);

/* ============================================================================
 * Memory Pool Implementation
 * ============================================================================ */

static int
ioperf_mempool_create(struct ioperf_disk *disk, uint32_t count)
{
    /* Round count to power of 2 */
    count = 1;
    while (count < 4096) {
        count <<= 1;
    }

    disk->io_pool.entries = calloc(count, sizeof(struct ioperf_io_ctx));
    if (!disk->io_pool.entries) {
        return -ENOMEM;
    }

    disk->io_pool.count = count;
    disk->io_pool.mask = count - 1;
    atomic_init(&disk->io_pool.alloc_idx, 0);

    return 0;
}

static void
ioperf_mempool_destroy(struct ioperf_disk *disk)
{
    if (disk->io_pool.entries) {
        free(disk->io_pool.entries);
        disk->io_pool.entries = NULL;
    }
}

/* ============================================================================
 * Hash Map Implementation
 * ============================================================================ */

static int
ioperf_hash_map_init(struct ioperf_hash_map *hash_map, size_t size)
{
    hash_map->size = size;

    if (pthread_rwlock_init(&hash_map->lock, NULL) != 0) {
        return -1;
    }

    hash_map->keys = calloc(size, sizeof(int));
    if (!hash_map->keys) {
        pthread_rwlock_destroy(&hash_map->lock);
        return -1;
    }

    hash_map->values = calloc(size, sizeof(int));
    if (!hash_map->values) {
        free(hash_map->keys);
        pthread_rwlock_destroy(&hash_map->lock);
        return -1;
    }

    /* Initialize with sequential values for testing */
    for (size_t i = 0; i < size; i++) {
        hash_map->keys[i] = (int)i;
        hash_map->values[i] = (int)(i * 2);
    }

    return 0;
}

static void
ioperf_hash_map_destroy(struct ioperf_hash_map *hash_map)
{
    if (hash_map->keys) {
        free(hash_map->keys);
        hash_map->keys = NULL;
    }
    if (hash_map->values) {
        free(hash_map->values);
        hash_map->values = NULL;
    }
    pthread_rwlock_destroy(&hash_map->lock);
}

static uint32_t
ioperf_hash_lba(uint64_t lba, uint32_t num_threads)
{
    /* Simple hash: (lba * prime) % num_threads
     * prime = 2654435761 (Knuth's golden ratio)
     */
    return (uint32_t)((lba * 2654435761ULL) % num_threads);
}

/* ============================================================================
 * Channel Management
 * ============================================================================ */

struct ioperf_channel *
ioperf_get_channel(struct ioperf_disk *disk, uint32_t thread_id)
{
    if (!disk || thread_id >= disk->channel_count) {
        return NULL;
    }
    return disk->channels[thread_id];
}

void
ioperf_put_channel(struct ioperf_channel *ch)
{
    (void)ch;
    /* No-op for now */
}

/* ============================================================================
 * IO Processing
 * ============================================================================ */

static void
fill_all_fields(struct ioperf_io_ctx *ctx, uint64_t io_size)
{
    ctx->field_001 = ctx->offset_blocks;
    ctx->field_002 = ctx->num_blocks;
    ctx->field_003 = io_size;
    ctx->field_004 = ctx->type;
    ctx->field_005 = ctx->target_thread;
    ctx->field_006 = ctx->hash_map_value_1;
    ctx->field_007 = ctx->hash_map_value_2;
    ctx->field_008 = ioperf_get_ticks();
    ctx->field_009 = io_size;
    ctx->field_010 = ctx->num_blocks * io_size;
    ctx->field_011 = ctx->field_001 + ctx->field_002;
    ctx->field_012 = ctx->field_003 + ctx->field_004;
    ctx->field_013 = ctx->field_005 + ctx->field_006;
    ctx->field_014 = ctx->field_007 + ctx->field_008;
    ctx->field_015 = ctx->field_009 + ctx->field_010;
    ctx->field_016 = ctx->field_001 * 2;
    ctx->field_017 = ctx->field_002 * 2;
    ctx->field_018 = ctx->field_003 * 2;
    ctx->field_019 = ctx->field_004 * 2;
    ctx->field_020 = ctx->field_005 * 2;
    ctx->field_021 = ctx->field_006 * 2;
    ctx->field_022 = ctx->field_007 * 2;
    ctx->field_023 = ctx->field_008 * 2;
    ctx->field_024 = ctx->field_009 * 2;
    ctx->field_025 = ctx->field_010 * 2;
    ctx->field_026 = ctx->field_001 - ctx->field_002;
    ctx->field_027 = ctx->field_003 - ctx->field_004;
    ctx->field_028 = ctx->field_005 - ctx->field_006;
    ctx->field_029 = ctx->field_007 - ctx->field_008;
    ctx->field_030 = ctx->field_009 - ctx->field_010;
    ctx->field_031 = ctx->field_001 & 0xFF;
    ctx->field_032 = ctx->field_002 & 0xFF;
    ctx->field_033 = ctx->field_003 & 0xFF;
    ctx->field_034 = ctx->field_004 & 0xFF;
    ctx->field_035 = ctx->field_005 & 0xFF;
    ctx->field_036 = ctx->field_006 | 0xFF;
    ctx->field_037 = ctx->field_007 | 0xFF;
    ctx->field_038 = ctx->field_008 | 0xFF;
    ctx->field_039 = ctx->field_009 | 0xFF;
    ctx->field_040 = ctx->field_010 | 0xFF;
    ctx->field_041 = ctx->field_001 ^ ctx->field_002;
    ctx->field_042 = ctx->field_003 ^ ctx->field_004;
    ctx->field_043 = ctx->field_005 ^ ctx->field_006;
    ctx->field_044 = ctx->field_007 ^ ctx->field_008;
    ctx->field_045 = ctx->field_009 ^ ctx->field_010;
    ctx->field_046 = ctx->field_001 << 1;
    ctx->field_047 = ctx->field_002 << 1;
    ctx->field_048 = ctx->field_003 << 1;
    ctx->field_049 = ctx->field_004 << 1;
    ctx->field_050 = ctx->field_005 << 1;
    ctx->field_051 = ctx->field_006 >> 1;
    ctx->field_052 = ctx->field_007 >> 1;
    ctx->field_053 = ctx->field_008 >> 1;
    ctx->field_054 = ctx->field_009 >> 1;
    ctx->field_055 = ctx->field_010 >> 1;
    ctx->field_056 = ctx->field_001 + ctx->field_003;
    ctx->field_057 = ctx->field_002 + ctx->field_004;
    ctx->field_058 = ctx->field_005 + ctx->field_007;
    ctx->field_059 = ctx->field_006 + ctx->field_008;
    ctx->field_060 = ctx->field_009 + ctx->field_010;
    ctx->field_061 = ctx->field_001 * ctx->field_002;
    ctx->field_062 = ctx->field_003 * ctx->field_004;
    ctx->field_063 = ctx->field_005 * ctx->field_006;
    ctx->field_064 = ctx->field_007 * ctx->field_008;
    ctx->field_065 = ctx->field_009 * ctx->field_010;
    ctx->field_066 = ctx->field_001 % 100;
    ctx->field_067 = ctx->field_002 % 100;
    ctx->field_068 = ctx->field_003 % 100;
    ctx->field_069 = ctx->field_004 % 100;
    ctx->field_070 = ctx->field_005 % 100;
    ctx->field_071 = ctx->field_001 / 2;
    ctx->field_072 = ctx->field_002 / 2;
    ctx->field_073 = ctx->field_003 / 2;
    ctx->field_074 = ctx->field_004 / 2;
    ctx->field_075 = ctx->field_005 / 2;
    ctx->field_076 = ctx->field_001 + 1;
    ctx->field_077 = ctx->field_002 + 1;
    ctx->field_078 = ctx->field_003 + 1;
    ctx->field_079 = ctx->field_004 + 1;
    ctx->field_080 = ctx->field_005 + 1;
    ctx->field_081 = ctx->field_001 - 1;
    ctx->field_082 = ctx->field_002 - 1;
    ctx->field_083 = ctx->field_003 - 1;
    ctx->field_084 = ctx->field_004 - 1;
    ctx->field_085 = ctx->field_005 - 1;
    ctx->field_086 = ctx->field_006 + ctx->field_007;
    ctx->field_087 = ctx->field_008 + ctx->field_009;
    ctx->field_088 = ctx->field_010 + ctx->field_001;
    ctx->field_089 = ctx->field_002 + ctx->field_003;
    ctx->field_090 = ctx->field_004 + ctx->field_005;
    ctx->field_091 = ctx->field_006 * ctx->field_007;
    ctx->field_092 = ctx->field_008 * ctx->field_009;
    ctx->field_093 = ctx->field_010 * ctx->field_001;
    ctx->field_094 = ctx->field_002 * ctx->field_003;
    ctx->field_095 = ctx->field_004 * ctx->field_005;
    ctx->field_096 = ctx->field_006 - ctx->field_007;
    ctx->field_097 = ctx->field_008 - ctx->field_009;
    ctx->field_098 = ctx->field_010 - ctx->field_001;
    ctx->field_099 = ctx->field_002 - ctx->field_003;
    ctx->field_100 = ctx->field_004 - ctx->field_005;
    ctx->field_101 = ctx->field_006 & ctx->field_007;
    ctx->field_102 = ctx->field_008 & ctx->field_009;
    ctx->field_103 = ctx->field_010 & ctx->field_001;
    ctx->field_104 = ctx->field_002 & ctx->field_003;
    ctx->field_105 = ctx->field_004 & ctx->field_005;
    ctx->field_106 = ctx->field_006 | ctx->field_007;
    ctx->field_107 = ctx->field_008 | ctx->field_009;
    ctx->field_108 = ctx->field_010 | ctx->field_001;
    ctx->field_109 = ctx->field_002 | ctx->field_003;
    ctx->field_110 = ctx->field_004 | ctx->field_005;
    ctx->field_111 = ctx->field_006 ^ ctx->field_007;
    ctx->field_112 = ctx->field_008 ^ ctx->field_009;
    ctx->field_113 = ctx->field_010 ^ ctx->field_001;
    ctx->field_114 = ctx->field_002 ^ ctx->field_003;
    ctx->field_115 = ctx->field_004 ^ ctx->field_005;
    ctx->field_116 = ctx->field_001 << 2;
    ctx->field_117 = ctx->field_002 << 2;
    ctx->field_118 = ctx->field_003 << 2;
    ctx->field_119 = ctx->field_004 << 2;
    ctx->field_120 = ctx->field_005 << 2;
    ctx->field_121 = ctx->field_006 >> 2;
    ctx->field_122 = ctx->field_007 >> 2;
    ctx->field_123 = ctx->field_008 >> 2;
    ctx->field_124 = ctx->field_009 >> 2;
    ctx->field_125 = ctx->field_010 >> 2;
    ctx->field_126 = ctx->field_001 + ctx->field_002 + ctx->field_003;
    ctx->field_127 = ctx->field_004 + ctx->field_005 + ctx->field_006;
    ctx->field_128 = ioperf_get_ticks();
}

static bool
rate_limit_check(struct ioperf_channel *ch, struct ioperf_disk *disk, uint64_t io_size)
{
    if (disk == NULL) {
        return true;
    }

    uint64_t *last_time_ptr = &ch->last_time;
    uint64_t *token_bucket_ptr = &ch->token_bucket;

    /* Initialize last_time on first call */
    if (*last_time_ptr == 0) {
        *last_time_ptr = ioperf_get_ticks();
        /* Give initial tokens to allow IO to proceed */
        *token_bucket_ptr = disk->max_bandwidth_bytes;
    }

    uint64_t now = ioperf_get_ticks();
    uint64_t elapsed = now - *last_time_ptr;

    if (elapsed > 0) {
        /* Refill token bucket */
        uint64_t ticks_per_second = ioperf_get_ticks_hz();
        uint64_t tokens_to_add = (disk->max_bandwidth_bytes / ticks_per_second) * elapsed;
        *token_bucket_ptr += tokens_to_add;
        *last_time_ptr = now;
    }

    /* Check rate limit */
    if (ch->wait_queue_count < MAX_QUEUED_IO && *token_bucket_ptr >= io_size) {
        *token_bucket_ptr -= io_size;
        return true;
    }

    return false;
}

static void
ioperf_process_io(struct ioperf_io_ctx *io_ctx)
{
    struct ioperf_disk *disk;

    if (!io_ctx || !io_ctx->channel) {
        return;
    }

    disk = io_ctx->channel->disk;
    if (!disk) {
        return;
    }

    /* Fill all fields to simulate memory access */
    fill_all_fields(io_ctx, io_ctx->io_size);

    /* Update stats */
    disk->total_io++;
    disk->total_bytes += io_ctx->io_size;

    /* Complete the IO */
    ioperf_io_complete(io_ctx, IOPERF_IO_STATUS_SUCCESS);
}

void
ioperf_io_complete(struct ioperf_io_ctx *io, enum ioperf_io_status status)
{
    if (io->complete) {
        io->complete(io->complete_arg);
    }
    (void)status;
}

void
ioperf_io_ctx_init(struct ioperf_io_ctx *io)
{
    memset(io, 0, sizeof(*io));
}

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
        pprev = &ch->rate_limit_queue_head;
        io_ctx = ch->rate_limit_queue_head;
        while (io_ctx) {
            if (rate_limit_check(ch, disk, io_ctx->io_size)) {
                *pprev = io_ctx->next;
                if (ch->rate_limit_queue_tail == io_ctx) {
                    ch->rate_limit_queue_tail = *pprev;
                }
                ch->rate_limit_queue_count--;
                ioperf_process_io(io_ctx);
                io_ctx = *pprev;
            } else {
                pprev = &io_ctx->next;
                io_ctx = io_ctx->next;
            }
        }
        /* No nanosleep - busy poll continuously */
    }

    return NULL;
}

/* ============================================================================
 * Public API
 * ============================================================================ */

int
ioperf_submit_io(struct ioperf_disk *disk, struct ioperf_io_ctx *io)
{
    struct ioperf_channel *ch;
    uint64_t delay_ns;
    uint32_t thread_id;

    if (!disk || !io) {
        return -EINVAL;
    }

    /* Calculate delay based on IO type */
    if (io->type == IOPERF_IO_READ) {
        delay_ns = disk->read_latency_ns;
    } else if (io->type == IOPERF_IO_WRITE) {
        delay_ns = disk->write_latency_ns;
    } else {
        delay_ns = 0;
    }

    /* Calculate target thread based on LBA */
    thread_id = ioperf_hash_lba(io->offset_blocks, disk->num_threads);
    io->target_thread = thread_id;
    io->io_size = io->num_blocks * disk->block_size;

    /* Get channel */
    ch = disk->channels[thread_id];
    if (!ch) {
        return -EINVAL;
    }
    io->channel = ch;

    /* Fill hash map values */
    io->hash_map_value_1 = (int)(io->offset_blocks % disk->hash_map_1.size);
    io->hash_map_value_2 = (int)((io->offset_blocks / 1000) % disk->hash_map_2.size);

    /* If no delay, complete immediately */
    if (delay_ns == 0) {
        ioperf_process_io(io);
        return 0;
    }

    /* Check rate limit */
    if (!rate_limit_check(ch, disk, io->io_size)) {
        /* Add to rate limit queue */
        io->next = NULL;
        if (ch->rate_limit_queue_tail) {
            ch->rate_limit_queue_tail->next = io;
            ch->rate_limit_queue_tail = io;
        } else {
            ch->rate_limit_queue_head = io;
            ch->rate_limit_queue_tail = io;
        }
        ch->rate_limit_queue_count++;
        return 0;
    }

    /* Add to wait queue with delay */
    io->queued_ticks = ioperf_get_ticks();
    io->next = NULL;
    if (ch->wait_queue_tail) {
        ch->wait_queue_tail->next = io;
        ch->wait_queue_tail = io;
    } else {
        ch->wait_queue_head = io;
        ch->wait_queue_tail = io;
    }
    ch->wait_queue_count++;

    return 0;
}

int
ioperf_disk_create(struct ioperf_disk **out_disk, const struct ioperf_opts *opts)
{
    struct ioperf_disk *disk;
    int rc;
    uint32_t i;

    if (!out_disk || !opts) {
        return -EINVAL;
    }

    if (opts->num_blocks == 0) {
        return -EINVAL;
    }

    if (opts->block_size % 512 != 0) {
        return -EINVAL;
    }

    if (opts->physical_block_size % 512 != 0) {
        return -EINVAL;
    }

    disk = calloc(1, sizeof(*disk));
    if (!disk) {
        return -ENOMEM;
    }

    /* Copy name */
    strncpy(disk->name, opts->name ? opts->name : "ioperf", sizeof(disk->name) - 1);

    /* Copy configuration */
    disk->block_size = opts->block_size;
    disk->physical_block_size = opts->physical_block_size;
    disk->block_count = opts->num_blocks;
    disk->num_threads = opts->num_threads > 0 ? opts->num_threads : 4;
    disk->read_latency_ns = opts->read_latency_us * 1000;
    disk->write_latency_ns = opts->write_latency_us * 1000;
    disk->enable_validation = opts->enable_validation;

    /* Copy poller CPU configuration */
    disk->poller_cpus_count = opts->poller_cpus_count;
    if (opts->poller_cpus_count > 0 && opts->poller_cpus != NULL) {
        disk->poller_cpus = calloc(opts->poller_cpus_count, sizeof(uint32_t));
        if (disk->poller_cpus == NULL) {
            free(disk);
            return -ENOMEM;
        }
        memcpy(disk->poller_cpus, opts->poller_cpus,
               opts->poller_cpus_count * sizeof(uint32_t));
    } else {
        disk->poller_cpus = NULL;
    }

    disk->max_iops = IOPERF_MAX_IOPS;
    disk->max_bandwidth_bytes = (uint64_t)IOPERF_MAX_BANDWIDTH_MB * 1024 * 1024;

    /* Initialize hash maps */
    rc = ioperf_hash_map_init(&disk->hash_map_1, IOPERF_HASH_MAP_SIZE);
    if (rc != 0) {
        free(disk);
        return rc;
    }

    rc = ioperf_hash_map_init(&disk->hash_map_2, IOPERF_HASH_MAP_SIZE);
    if (rc != 0) {
        ioperf_hash_map_destroy(&disk->hash_map_1);
        free(disk);
        return rc;
    }

    /* Create memory pool */
    rc = ioperf_mempool_create(disk, 8192);
    if (rc != 0) {
        ioperf_hash_map_destroy(&disk->hash_map_1);
        ioperf_hash_map_destroy(&disk->hash_map_2);
        free(disk);
        return rc;
    }

    /* Create channels */
    disk->channels = calloc(disk->num_threads, sizeof(struct ioperf_channel *));
    if (!disk->channels) {
        ioperf_mempool_destroy(disk);
        ioperf_hash_map_destroy(&disk->hash_map_1);
        ioperf_hash_map_destroy(&disk->hash_map_2);
        free(disk);
        return -ENOMEM;
    }

    for (i = 0; i < disk->num_threads; i++) {
        disk->channels[i] = calloc(1, sizeof(struct ioperf_channel));
        if (!disk->channels[i]) {
            for (uint32_t j = 0; j < i; j++) {
                free(disk->channels[j]);
            }
            free(disk->channels);
            ioperf_mempool_destroy(disk);
            ioperf_hash_map_destroy(&disk->hash_map_1);
            ioperf_hash_map_destroy(&disk->hash_map_2);
            free(disk);
            return -ENOMEM;
        }

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
    }

    disk->channel_count = disk->num_threads;

    *out_disk = disk;
    return 0;
}

void
ioperf_disk_destroy(struct ioperf_disk *disk)
{
    uint32_t i;

    if (!disk) {
        return;
    }

    /* Stop all poller threads */
    for (i = 0; i < disk->channel_count; i++) {
        if (disk->channels[i]) {
            disk->channels[i]->poller_running = false;
            pthread_join(disk->channels[i]->poller_thread, NULL);
            free(disk->channels[i]);
        }
    }

    free(disk->channels);

    /* Free poller_cpus array */
    if (disk->poller_cpus) {
        free(disk->poller_cpus);
        disk->poller_cpus = NULL;
    }

    ioperf_mempool_destroy(disk);
    ioperf_hash_map_destroy(&disk->hash_map_1);
    ioperf_hash_map_destroy(&disk->hash_map_2);

    free(disk);
}

int
ioperf_disk_resize(struct ioperf_disk *disk, uint64_t new_size_mb)
{
    if (!disk) {
        return -EINVAL;
    }

    disk->block_count = (new_size_mb * 1024 * 1024) / disk->block_size;
    return 0;
}

uint64_t
ioperf_disk_get_total_io(struct ioperf_disk *disk)
{
    if (!disk) {
        return 0;
    }
    return disk->total_io;
}

uint64_t
ioperf_disk_get_total_bytes(struct ioperf_disk *disk)
{
    if (!disk) {
        return 0;
    }
    return disk->total_bytes;
}
