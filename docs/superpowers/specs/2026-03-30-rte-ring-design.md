# rte_ring Implementation Design

**Date:** 2026-03-30
**Status:** Draft

## 1. Overview

Port a simplified version of DPDK's `rte_ring` (lock-free MPMC queue) and add comprehensive unit tests.

## 2. Goals

1. Implement MPMC lock-free ring buffer based on DPDK rte_ring algorithm
2. Add complete unit tests using Google Test

## 3. Architecture

### 3.1 rte_ring Structure

```c
struct rte_ring {
    char name[32];
    uint32_t capacity;
    uint32_t mask;
    _Atomic uint32_t prod_tail;
    _Atomic uint32_t prod_head;
    _Atomic uint32_t cons_tail;
    _Atomic uint32_t cons_head;
    void **entries;
};
```

### 3.2 Lock-Free Algorithm

Uses C11 atomic operations with CAS for MPMC:

- **Producer**: acquire tail, write to ring, update prod_tail
- **Consumer**: acquire head, read from ring, update cons_head
- Uses memory ordering barriers for correct ordering

## 4. API Design

### 4.1 Core Functions

```c
struct rte_ring *rte_ring_create(const char *name, uint32_t count);
void rte_ring_free(struct rte_ring *r);
int rte_ring_enqueue(struct rte_ring *r, void *obj);
int rte_ring_dequeue(struct rte_ring *r, void **obj);
uint32_t rte_ring_count(struct rte_ring *r);
uint32_t rte_ring_free_count(struct rte_ring *r);
int rte_ring_full(struct rte_ring *r);
int rte_ring_empty(struct rte_ring *r);
const char *rte_ring_get_name(struct rte_ring *r);
```

### 4.2 Bulk Operations

```c
int rte_ring_enqueue_bulk(struct rte_ring *r, void **objs, uint32_t n);
int rte_ring_dequeue_bulk(struct rte_ring *r, void **objs, uint32_t n);
```

## 5. Test Coverage

### 5.1 rte_ring Tests

| Test Case | Description |
|----------|-------------|
| Create/Destroy | Ring creation with valid/invalid parameters |
| Single Producer/Single Consumer | Basic enqueue/dequeue |
| Multi Producer/Multi Consumer | Concurrent access from multiple threads |
| Full Queue | Enqueue when ring is full returns error |
| Empty Queue | Dequeue when ring is empty returns error |
| Wrap Around | Test ring buffer wraps correctly |
| Data Integrity | Enqueued data matches dequeued data |
| Memory Ordering | Correct behavior under weak memory ordering |

### 5.2 Existing Module Tests

| Module | Test Coverage |
|--------|-------------|
| DIF | Guard tag calculation, verification |
| Rate Limit | IOPS limit, bandwidth limit |
| Memory Pool | Allocation, release |
| Thread Dispatch | LBA to thread mapping |

### 5.3 Test Framework

- **Framework:** Google Test (gtest)
- **Build:** CMake with gtest as external dependency
- **Output:** Standard output + exit code

## 6. File Structure

```
lib/test_bdev/
├── test_bdev.h      # Existing header
├── test_bdev.c     # Existing implementation
├── rte_ring.h      # New ring header
├── rte_ring.c      # New ring implementation

test/
├── standalone_test.c  # Existing standalone test
├── CMakeLists.txt  # Updated build config
└── ring_test.cc   # New gtest unit tests
```

## 7. Dependencies

- CMake >= 3.16
- Google Test (fetched via CMake)
- pthread
- Standard C11 atomics

## 8. Implementation Notes

- Round capacity up to power of 2 for fast modulo
- Use `memory_order_seq_cst` for simplicity (can optimize later)
- Follow existing code style in lib/test_bdev/