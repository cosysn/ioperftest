/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2024
 */

#ifndef IOPERF_H
#define IOPERF_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <stdatomic.h>

/* Compile-time configuration macros */
#ifndef IOPERF_MAX_IOPS
#define IOPERF_MAX_IOPS           1000000
#endif

#ifndef IOPERF_MAX_BANDWIDTH_MB
#define IOPERF_MAX_BANDWIDTH_MB   100000
#endif

#ifndef IOPERF_DEFAULT_READ_LATENCY_US
#define IOPERF_DEFAULT_READ_LATENCY_US   0
#endif

#ifndef IOPERF_DEFAULT_WRITE_LATENCY_US
#define IOPERF_DEFAULT_WRITE_LATENCY_US  0
#endif

#ifndef IOPERF_IO_CTX_FIELDS
#define IOPERF_IO_CTX_FIELDS      128
#endif

#ifndef IOPERF_HASH_MAP_SIZE
#define IOPERF_HASH_MAP_SIZE      1000000
#endif

/* Forward declarations */
struct ioperf_disk;
struct ioperf_io_ctx;

/* Time utilities */
static inline uint64_t
ioperf_get_ticks(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static inline uint64_t
ioperf_get_ticks_hz(void)
{
    return 1000000000ULL; /* nanoseconds */
}

/* Hash map structure */
struct ioperf_hash_map {
    pthread_rwlock_t    lock;
    int                *keys;
    int                *values;
    size_t             size;
};

/* IO types */
enum ioperf_io_type {
    IOPERF_IO_READ,
    IOPERF_IO_WRITE,
    IOPERF_IO_WRITE_ZEROES,
    IOPERF_IO_RESET,
    IOPERF_IO_ABORT,
};

/* IO status */
enum ioperf_io_status {
    IOPERF_IO_STATUS_SUCCESS,
    IOPERF_IO_STATUS_FAILED,
};

/* IO request structure - allocated from memory pool */
struct ioperf_io_ctx {
    /* Queue linkage */
    struct ioperf_io_ctx *next;

    /* IO info */
    enum ioperf_io_type  type;
    uint64_t             offset_blocks;
    uint64_t             num_blocks;
    uint64_t             io_size;  /* in bytes */

    /* Completion callback */
    void                (*complete)(void *arg);
    void                *complete_arg;

    /* Channel reference for async callbacks */
    struct ioperf_channel *channel;

    /* Routing info */
    uint32_t            target_thread;

    /* Delay timestamp */
    uint64_t            queued_ticks;

    /* Hash map values */
    int                 hash_map_value_1;
    int                 hash_map_value_2;

    /* 100+ fields for memory access simulation */
    uint64_t            field_001;
    uint64_t            field_002;
    uint64_t            field_003;
    uint64_t            field_004;
    uint64_t            field_005;
    uint64_t            field_006;
    uint64_t            field_007;
    uint64_t            field_008;
    uint64_t            field_009;
    uint64_t            field_010;
    uint64_t            field_011;
    uint64_t            field_012;
    uint64_t            field_013;
    uint64_t            field_014;
    uint64_t            field_015;
    uint64_t            field_016;
    uint64_t            field_017;
    uint64_t            field_018;
    uint64_t            field_019;
    uint64_t            field_020;
    uint64_t            field_021;
    uint64_t            field_022;
    uint64_t            field_023;
    uint64_t            field_024;
    uint64_t            field_025;
    uint64_t            field_026;
    uint64_t            field_027;
    uint64_t            field_028;
    uint64_t            field_029;
    uint64_t            field_030;
    uint64_t            field_031;
    uint64_t            field_032;
    uint64_t            field_033;
    uint64_t            field_034;
    uint64_t            field_035;
    uint64_t            field_036;
    uint64_t            field_037;
    uint64_t            field_038;
    uint64_t            field_039;
    uint64_t            field_040;
    uint64_t            field_041;
    uint64_t            field_042;
    uint64_t            field_043;
    uint64_t            field_044;
    uint64_t            field_045;
    uint64_t            field_046;
    uint64_t            field_047;
    uint64_t            field_048;
    uint64_t            field_049;
    uint64_t            field_050;
    uint64_t            field_051;
    uint64_t            field_052;
    uint64_t            field_053;
    uint64_t            field_054;
    uint64_t            field_055;
    uint64_t            field_056;
    uint64_t            field_057;
    uint64_t            field_058;
    uint64_t            field_059;
    uint64_t            field_060;
    uint64_t            field_061;
    uint64_t            field_062;
    uint64_t            field_063;
    uint64_t            field_064;
    uint64_t            field_065;
    uint64_t            field_066;
    uint64_t            field_067;
    uint64_t            field_068;
    uint64_t            field_069;
    uint64_t            field_070;
    uint64_t            field_071;
    uint64_t            field_072;
    uint64_t            field_073;
    uint64_t            field_074;
    uint64_t            field_075;
    uint64_t            field_076;
    uint64_t            field_077;
    uint64_t            field_078;
    uint64_t            field_079;
    uint64_t            field_080;
    uint64_t            field_081;
    uint64_t            field_082;
    uint64_t            field_083;
    uint64_t            field_084;
    uint64_t            field_085;
    uint64_t            field_086;
    uint64_t            field_087;
    uint64_t            field_088;
    uint64_t            field_089;
    uint64_t            field_090;
    uint64_t            field_091;
    uint64_t            field_092;
    uint64_t            field_093;
    uint64_t            field_094;
    uint64_t            field_095;
    uint64_t            field_096;
    uint64_t            field_097;
    uint64_t            field_098;
    uint64_t            field_099;
    uint64_t            field_100;
    uint64_t            field_101;
    uint64_t            field_102;
    uint64_t            field_103;
    uint64_t            field_104;
    uint64_t            field_105;
    uint64_t            field_106;
    uint64_t            field_107;
    uint64_t            field_108;
    uint64_t            field_109;
    uint64_t            field_110;
    uint64_t            field_111;
    uint64_t            field_112;
    uint64_t            field_113;
    uint64_t            field_114;
    uint64_t            field_115;
    uint64_t            field_116;
    uint64_t            field_117;
    uint64_t            field_118;
    uint64_t            field_119;
    uint64_t            field_120;
    uint64_t            field_121;
    uint64_t            field_122;
    uint64_t            field_123;
    uint64_t            field_124;
    uint64_t            field_125;
    uint64_t            field_126;
    uint64_t            field_127;
    uint64_t            field_128;
};

/* IO channel structure (per thread) */
struct ioperf_channel {
    /* Wait queue for IO waiting for delay */
    struct ioperf_io_ctx *wait_queue_head;
    struct ioperf_io_ctx *wait_queue_tail;
    uint32_t              wait_queue_count;

    /* Rate limit queue */
    struct ioperf_io_ctx *rate_limit_queue_head;
    struct ioperf_io_ctx *rate_limit_queue_tail;
    uint32_t              rate_limit_queue_count;

    /* Rate limiting state */
    uint64_t              last_time;
    uint64_t              token_bucket;

    /* Config */
    uint32_t              thread_id;

    /* Reference to disk */
    struct ioperf_disk   *disk;

    /* Poller state */
    pthread_t             poller_thread;
    bool                 poller_running;
};

/* ioperf disk structure */
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
    uint64_t             max_iops;
    uint64_t             max_bandwidth_bytes;
    bool                 enable_validation;

    /* Global stats */
    uint64_t             total_io;
    uint64_t             total_bytes;

    /* Memory pool for IO requests */
    struct {
        struct ioperf_io_ctx *entries;
        uint32_t             count;
        uint32_t             mask;
        _Atomic uint32_t     alloc_idx;
    } io_pool;

    /* Hash maps for testing read lock performance */
    struct ioperf_hash_map hash_map_1;
    struct ioperf_hash_map hash_map_2;

    /* Channels array */
    struct ioperf_channel **channels;
    uint32_t               channel_count;
};

/* Options for creating ioperf disk */
struct ioperf_opts {
    char     *name;
    uint64_t  num_blocks;
    uint32_t  block_size;
    uint32_t  physical_block_size;
    uint32_t  num_threads;
    uint32_t *poller_cpus;       /* CPU IDs for poller affinity, e.g. {0, 2, 4, 6} */
    uint32_t  poller_cpus_count; /* length of poller_cpus array */
    uint64_t  read_latency_us;
    uint64_t  write_latency_us;
    bool      enable_validation;
};

/* API functions */
int  ioperf_disk_create(struct ioperf_disk **disk, const struct ioperf_opts *opts);
void ioperf_disk_destroy(struct ioperf_disk *disk);
int  ioperf_disk_resize(struct ioperf_disk *disk, uint64_t new_size_mb);

/* IO submission */
int  ioperf_submit_io(struct ioperf_disk *disk, struct ioperf_io_ctx *io);
void ioperf_io_complete(struct ioperf_io_ctx *io, enum ioperf_io_status status);

/* Initialize IO context */
void ioperf_io_ctx_init(struct ioperf_io_ctx *io);

/* Channel management */
struct ioperf_channel *ioperf_get_channel(struct ioperf_disk *disk, uint32_t thread_id);
void ioperf_put_channel(struct ioperf_channel *ch);

/* Utility */
uint64_t ioperf_disk_get_total_io(struct ioperf_disk *disk);
uint64_t ioperf_disk_get_total_bytes(struct ioperf_disk *disk);

#endif /* IOPERF_H */
