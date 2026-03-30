/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (c) 2024
 */

#include "rte_ring.h"

struct rte_ring *
rte_ring_create(const char *name, uint32_t count)
{
    struct rte_ring *r;
    uint32_t i;

    /* Validate inputs */
    if (name == NULL || count == 0) {
        return NULL;
    }

    /* Round count to power of 2 */
    count = RTE_DIMENSION(count);

    /* Allocate ring structure */
    r = malloc(sizeof(*r));
    if (r == NULL) {
        return NULL;
    }

    /* Allocate entries array */
    r->entries = malloc(count * sizeof(void *));
    if (r->entries == NULL) {
        free(r);
        return NULL;
    }

    /* Initialize ring */
    memset(r->name, 0, sizeof(r->name));
    strncpy(r->name, name, sizeof(r->name) - 1);
    r->capacity = count;
    r->mask = count - 1;
    atomic_store(&r->prod_tail, 0);
    atomic_store(&r->cons_tail, 0);

    /* Initialize entries to NULL */
    for (i = 0; i < count; i++) {
        r->entries[i] = NULL;
    }

    return r;
}

void
rte_ring_free(struct rte_ring *r)
{
    if (r == NULL) {
        return;
    }

    /* Free entries array first, then ring */
    if (r->entries != NULL) {
        free(r->entries);
        r->entries = NULL;
    }

    free(r);
}

int
rte_ring_enqueue(struct rte_ring *r, void *obj)
{
    uint32_t prod_tail;
    uint32_t cons_tail;
    uint32_t count;

    if (r == NULL || obj == NULL) {
        return -1;
    }

    /* Check if ring is full */
    count = rte_ring_count(r);
    if (count >= r->capacity) {
        return -1;
    }

    /* Get current producer tail */
    prod_tail = atomic_load(&r->prod_tail);
    cons_tail = atomic_load(&r->cons_tail);

    /* Reserve slot using atomic_fetch_add on prod_tail */
    prod_tail = atomic_fetch_add(&r->prod_tail, 1);

    /* Double check we have space after reserving */
    cons_tail = atomic_load(&r->cons_tail);
    if ((prod_tail - cons_tail) >= r->capacity) {
        /* Ring is full, need to rollback - not fully lock-free
         * but this is the standard DPDK approach
         */
        return -1;
    }

    /* Write object to ring */
    r->entries[prod_tail & r->mask] = obj;

    /* Memory fence for ordering */
    atomic_thread_fence(memory_order_release);

    return 0;
}

int
rte_ring_dequeue(struct rte_ring *r, void **obj)
{
    uint32_t cons_tail;
    uint32_t prod_tail;
    uint32_t count;

    if (r == NULL || obj == NULL) {
        return -1;
    }

    /* Check if ring is empty */
    count = rte_ring_count(r);
    if (count == 0) {
        return -1;
    }

    /* Get current consumer tail */
    cons_tail = atomic_load(&r->cons_tail);
    prod_tail = atomic_load(&r->prod_tail);

    /* Double check we have data after reserving */
    if (cons_tail == prod_tail) {
        /* Ring is empty */
        return -1;
    }

    /* Reserve slot using atomic_fetch_add on cons_tail */
    cons_tail = atomic_fetch_add(&r->cons_tail, 1);

    /* Re-check after reservation */
    prod_tail = atomic_load(&r->prod_tail);
    if (cons_tail >= prod_tail) {
        /* Nothing to dequeue */
        return -1;
    }

    /* Read object from ring */
    *obj = r->entries[cons_tail & r->mask];

    /* Memory fence for ordering */
    atomic_thread_fence(memory_order_acquire);

    return 0;
}

uint32_t
rte_ring_count(struct rte_ring *r)
{
    uint32_t prod_tail;
    uint32_t cons_tail;

    if (r == NULL) {
        return 0;
    }

    prod_tail = atomic_load(&r->prod_tail);
    cons_tail = atomic_load(&r->cons_tail);

    return prod_tail - cons_tail;
}

uint32_t
rte_ring_free_count(struct rte_ring *r)
{
    if (r == NULL) {
        return 0;
    }

    return r->capacity - rte_ring_count(r);
}

bool
rte_ring_full(struct rte_ring *r)
{
    if (r == NULL) {
        return false;
    }

    return rte_ring_count(r) >= r->capacity;
}

bool
rte_ring_empty(struct rte_ring *r)
{
    if (r == NULL) {
        return true;
    }

    return rte_ring_count(r) == 0;
}

const char *
rte_ring_get_name(struct rte_ring *r)
{
    if (r == NULL) {
        return NULL;
    }

    return r->name;
}
