/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2024
 */

#ifndef RTE_RING_H
#define RTE_RING_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

/* Handle atomics for both C and C++ */
#ifndef __cplusplus
#include <stdatomic.h>
#else
#include <atomic>
#endif

/* Power-of-2 rounding macro */
#define RTE_DIMENSION(x) ( \
    __builtin_constant_p(x) ? \
    ((x) < 2 ? 1 : \
     1 << (64 - __builtin_clzl((x) - 1))) : \
    _rte_ring_round_pow2(x))

/* Helper function for non-constant values */
static inline uint32_t
_rte_ring_round_pow2(uint32_t x)
{
    x--;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    x++;
    return x;
}

/* rte_ring structure */
#ifdef __cplusplus
struct rte_ring {
    char name[32];
    uint32_t capacity;
    uint32_t mask;
    std::atomic<uint32_t> prod_tail;
    std::atomic<uint32_t> cons_tail;
    void **entries;
};
#else
struct rte_ring {
    char name[32];
    uint32_t capacity;
    uint32_t mask;
    _Atomic uint32_t prod_tail;
    _Atomic uint32_t cons_tail;
    void **entries;
};
#endif

/* API functions */
struct rte_ring *rte_ring_create(const char *name, uint32_t count);
void rte_ring_free(struct rte_ring *r);
int rte_ring_enqueue(struct rte_ring *r, void *obj);
int rte_ring_dequeue(struct rte_ring *r, void **obj);
uint32_t rte_ring_count(struct rte_ring *r);
uint32_t rte_ring_free_count(struct rte_ring *r);
bool rte_ring_full(struct rte_ring *r);
bool rte_ring_empty(struct rte_ring *r);
const char *rte_ring_get_name(struct rte_ring *r);
uint32_t rte_ring_capacity(struct rte_ring *r);

#endif /* RTE_RING_H */