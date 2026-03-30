# rte_ring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement MPMC lock-free ring buffer (simplified rte_ring) and add complete unit tests using Google Test.

**Architecture:** Based on DPDK rte_ring algorithm - MPMC lock-free ring buffer using C11 atomic CAS operations. Round capacity to power of 2 for fast modulo. Follow existing code style in lib/test_bdev/.

**Tech Stack:** C11, atomics, pthread, Google Test (gtest)

---

## File Structure

```
lib/test_bdev/
├── test_bdev.h      # Existing header - add rte_ring structures
├── test_bdev.c     # Existing implementation - no changes needed
├── rte_ring.h      # NEW: ring header
├── rte_ring.c      # NEW: ring implementation

test/
├── CMakeLists.txt  # NEW: CMake build with gtest
├── ring_test.cc  # NEW: gtest unit tests
```

---

## Task 1: Create rte_ring Header

**Files:**
- Create: `lib/test_bdev/rte_ring.h`

- [ ] **Step 1: Create rte_ring.h header**

```c
#ifndef RTE_RING_H
#define RTE_RING_H

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

/* Round x up to the next power of 2 */
#define RTE_DIMENSION(x) ( \
    __builtin_constant_p(x) ? \
    ((x) < 2 ? 1 : \
     1 << (64 - __builtin_clzl((x) - 1))) : \
    _rte_ring_round_pow2(x))

static inline uint32_t
_rte_ring_round_pow2(uint32_t x)
{
    x--;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    return ++x;
}

struct rte_ring {
    char name[32];
    uint32_t capacity;
    uint32_t mask;
    _Atomic uint32_t prod_tail;
    _Atomic uint32_t cons_tail;
    void **entries;
};

struct rte_ring *rte_ring_create(const char *name, uint32_t count);
void rte_ring_free(struct rte_ring *r);
int rte_ring_enqueue(struct rte_ring *r, void *obj);
int rte_ring_dequeue(struct rte_ring *r, void **obj);
uint32_t rte_ring_count(struct rte_ring *r);
uint32_t rte_ring_free_count(struct rte_ring *r);
int rte_ring_full(struct rte_ring *r);
int rte_ring_empty(struct rte_ring *r);
const char *rte_ring_get_name(struct rte_ring *r);

#endif
```

- [ ] **Step 2: Commit**

```bash
git add lib/test_bdev/rte_ring.h
git commit -m "feat: add rte_ring header file

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Implement rte_ring Core Functions

**Files:**
- Create: `lib/test_bdev/rte_ring.c`

- [ ] **Step 1: Create rte_ring.c with core implementation**

```c
/* SPDX-License-Identifier: BSD-3-Clause
 * Simplified rte_ring implementation based on DPDK
 */

#include "rte_ring.h"

struct rte_ring *
rte_ring_create(const char *name, uint32_t count)
{
    struct rte_ring *r;
    uint32_t i;

    if (count == 0 || name == NULL)
        return NULL;

    /* Round up to power of 2 */
    count = RTE_DIMENSION(count);

    r = calloc(1, sizeof(struct rte_ring));
    if (!r)
        return NULL;

    r->entries = calloc(count, sizeof(void *));
    if (!r->entries) {
        free(r);
        return NULL;
    }

    strncpy(r->name, name, sizeof(r->name) - 1);
    r->capacity = count;
    r->mask = count - 1;
    atomic_init(&r->prod_tail, 0);
    atomic_init(&r->cons_tail, 0);

    /* Initialize entries to NULL to distinguish empty from used */
    for (i = 0; i < count; i++)
        r->entries[i] = NULL;

    return r;
}

void
rte_ring_free(struct rte_ring *r)
{
    if (r) {
        free(r->entries);
        free(r);
    }
}

int
rte_ring_enqueue(struct rte_ring *r, void *obj)
{
    uint32_t prod_tail;
    uint32_t prod_next;
    uint32_t cons_tail;

    if (r == NULL || obj == NULL)
        return -1;

    prod_tail = atomic_load_explicit(&r->prod_tail, memory_order_relaxed);
    cons_tail = atomic_load_explicit(&r->cons_tail, memory_order_acquire);

    /* Check if ring is full */
    if ((prod_tail - cons_tail) >= r->mask + 1)
        return -1;

    prod_next = (prod_tail + 1) & r->mask;

    /* Try to reserve a slot using CAS */
    prod_next = (atomic_fetch_add(&r->prod_tail, 1) + 1) & r->mask;

    /* Check again after CAS */
    cons_tail = atomic_load(&r->cons_tail);
    if ((prod_next - cons_tail) >= r->mask + 1) {
        /* Ring is full, undo */
        return -1;
    }

    /* Write the object */
    r->entries[prod_next] = obj;

    /* Ensure write is visible before updating tail */
    atomic_thread_fence(memory_order_release);

    return 0;
}

int
rte_ring_dequeue(struct rte_ring *r, void **obj)
{
    uint32_t cons_tail;
    uint32_t prod_tail;
    uint32_t cons_next;

    if (r == NULL || obj == NULL)
        return -1;

    cons_tail = atomic_load_explicit(&r->cons_tail, memory_order_relaxed);
    prod_tail = atomic_load_explicit(&r->prod_tail, memory_order_acquire);

    /* Check if ring is empty */
    if (cons_tail == prod_tail)
        return -1;

    cons_next = (cons_tail + 1) & r->mask;

    /* Try to reserve a slot using CAS */
    cons_next = (atomic_fetch_add(&r->cons_tail, 1) + 1) & r->mask;

    /* Check again after CAS */
    prod_tail = atomic_load(&r->prod_tail);
    if (cons_next == prod_tail) {
        /* Ring is empty, undo */
        return -1;
    }

    /* Read the object */
    *obj = r->entries[cons_next];
    r->entries[cons_next] = NULL;

    /* Ensure read is complete before updating tail */
    atomic_thread_fence(memory_order_release);

    return 0;
}

uint32_t
rte_ring_count(struct rte_ring *r)
{
    uint32_t prod_tail = atomic_load(&r->prod_tail);
    uint32_t cons_tail = atomic_load(&r->cons_tail);
    return prod_tail - cons_tail;
}

uint32_t
rte_ring_free_count(struct rte_ring *r)
{
    return r->capacity - rte_ring_count(r);
}

int
rte_ring_full(struct rte_ring *r)
{
    return rte_ring_count(r) >= r->capacity;
}

int
rte_ring_empty(struct rte_ring *r)
{
    return rte_ring_count(r) == 0;
}

const char *
rte_ring_get_name(struct rte_ring *r)
{
    return r ? r->name : NULL;
}
```

- [ ] **Step 2: Add missing function for 32-bit systems**
Add this helper to the header (it uses __builtin_clzl which requires glibc):
```c
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
```
Update the macro to use this function.

- [ ] **Step 3: Commit**

```bash
git add lib/test_bdev/rte_ring.c
git commit -m "feat: implement rte_ring core functions

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Create CMake Build with Google Test

**Files:**
- Create: `test/CMakeLists.txt`

- [ ] **Step 1: Create CMakeLists.txt**

```cmake
cmake_minimum_required(VERSION 3.16)
project(ioperftest C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Find packages
find_package(Threads REQUIRED)
include(FetchContent)

# Fetch Google Test
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)
FetchContent_MakeAvailable(googletest)

enable_testing()

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/lib/test_bdev
    ${CMAKE_SOURCE_DIR}/include
)

# Build test executable
add_executable(ring_test
    ring_test.cc
)

target_link_libraries(ring_test
    gtest_main
    gtest
    Threads::Threads
)

target_compile_options(ring_test PRIVATE -Wall -Wextra -pthread)

# Run tests
add_test(NAME RingTest COMMAND ring_test)
```

- [ ] **Step 2: Commit**

```bash
git add test/CMakeLists.txt
git commit -m "feat: add CMakeLists.txt with Google Test

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 4: Write rte_ring Unit Tests

**Files:**
- Create: `test/ring_test.cc`

- [ ] **Step 1: Write unit tests**

```cpp
#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include "rte_ring.h"

class RteRingTest : public ::testing::Test {
protected:
    void SetUp() override {
        ring = nullptr;
    }
    void TearDown() override {
        if (ring)
            rte_ring_free(ring);
    }
    struct rte_ring *ring = nullptr;
};

TEST_F(RteRingTest, CreateValid) {
    ring = rte_ring_create("test", 64);
    ASSERT_NE(ring, nullptr);
    EXPECT_STREQ(rte_ring_get_name(ring), "test");
    EXPECT_EQ(rte_ring_capacity(ring), 64);
    EXPECT_TRUE(rte_ring_empty(ring));
}

TEST_F(RteRingTest, CreateInvalidNullCount) {
    ring = rte_ring_create("test", 0);
    EXPECT_EQ(ring, nullptr);
}

TEST_F(RteRingTest, CreateInvalidNullName) {
    ring = rte_ring_create(nullptr, 64);
    EXPECT_EQ(ring, nullptr);
}

TEST_F(RteRingTest, EnqueueDequeueSingle) {
    ring = rte_ring_create("test", 64);
    ASSERT_NE(ring, nullptr);

    void *obj = (void *)0x1234;
    EXPECT_EQ(rte_ring_enqueue(ring, obj), 0);
    EXPECT_EQ(rte_ring_count(ring), 1);

    void *out;
    EXPECT_EQ(rte_ring_dequeue(ring, &out), 0);
    EXPECT_EQ(out, obj);
    EXPECT_EQ(rte_ring_count(ring), 0);
}

TEST_F(RteRingTest, EnqueueFull) {
    ring = rte_ring_create("test", 4);
    ASSERT_NE(ring, nullptr);

    for (int i = 0; i < 4; i++) {
        void *obj = (void *)(uint64_t)(i + 1);
        EXPECT_EQ(rte_ring_enqueue(ring, obj), 0);
    }

    /* Ring is full, should fail */
    EXPECT_EQ(rte_ring_enqueue(ring, (void *)0x99), -1);
    EXPECT_TRUE(rte_ring_full(ring));
}

TEST_F(RteRingTest, DequeueEmpty) {
    ring = rte_ring_create("test", 64);
    ASSERT_NE(ring, nullptr);

    void *out;
    EXPECT_EQ(rte_ring_dequeue(ring, &out), -1);
    EXPECT_TRUE(rte_ring_empty(ring));
}

TEST_F(RteRingTest, WrapAround) {
    ring = rte_ring_create("test", 4);
    ASSERT_NE(ring, nullptr);

    /* Fill and drain */
    for (int iter = 0; iter < 3; iter++) {
        for (int i = 0; i < 4; i++) {
            void *obj = (void *)(uint64_t)(iter * 10 + i);
            EXPECT_EQ(rte_ring_enqueue(ring, obj), 0);
        }

        for (int i = 0; i < 4; i++) {
            void *out;
            EXPECT_EQ(rte_ring_dequeue(ring, &out), 0);
            EXPECT_EQ(out, (void *)(uint64_t)(iter * 10 + i));
        }
    }

    EXPECT_TRUE(rte_ring_empty(ring));
}

TEST_F(RteRingTest, DataIntegrity) {
    ring = rte_ring_create("test", 64);
    ASSERT_NE(ring, nullptr);

    std::string test_str = "hello world";
    void *obj = (void *)&test_str;

    EXPECT_EQ(rte_ring_enqueue(ring, obj), 0);

    void *out;
    EXPECT_EQ(rte_ring_dequeue(ring, &out), 0);
    EXPECT_EQ(out, obj);
    EXPECT_STREQ((const char *)out, "hello world");
}

TEST(RteRingConcurrent, MultiProducer) {
    struct rte_ring *r = rte_ring_create("concurrent", 1024);
    ASSERT_NE(r, nullptr);

    std::atomic<int> success_cnt{0};
    const int num_producers = 4;
    const int items_per_producer = 100;

    std::vector<std::thread> producers;
    for (int t = 0; t < num_producers; t++) {
        producers.emplace_back([r, t, &success_cnt]() {
            for (int i = 0; i < items_per_producer; i++) {
                void *obj = (void *)(uint64_t)(t * 1000 + i);
                if (rte_ring_enqueue(r, obj) == 0)
                    success_cnt++;
            }
        });
    }

    for (auto &th : producers)
        th.join();

    EXPECT_EQ(success_cnt, num_producers * items_per_producer);
    EXPECT_EQ(rte_ring_count(r), (uint32_t)(num_producers * items_per_producer));

    rte_ring_free(r);
}

TEST(RteRingConcurrent, MultiConsumer) {
    struct rte_ring *r = rte_ring_create("concurrent", 1024);
    ASSERT_NE(r, nullptr);

    const int num_items = 100;
    for (int i = 0; i < num_items; i++) {
        void *obj = (void *)(uint64_t)i;
        rte_ring_enqueue(r, obj);
    }

    std::atomic<int> success_cnt{0};
    const int num_consumers = 4;

    std::vector<std::thread> consumers;
    for (int t = 0; t < num_consumers; t++) {
        consumers.emplace_back([r, &success_cnt]() {
            void *obj;
            while (rte_ring_dequeue(r, &obj) == 0) {
                success_cnt++;
            }
        });
    }

    for (auto &th : consumers)
        th.join();

    EXPECT_EQ(success_cnt, num_items);
    EXPECT_TRUE(rte_ring_empty(r));

    rte_ring_free(r);
}

TEST(RteRingConcurrent, ProducerConsumer) {
    struct rte_ring *r = rte_ring_create("pc", 512);
    ASSERT_NE(r, nullptr);

    std::atomic<int> enqueue_cnt{0};
    std::atomic<int> dequeue_cnt{0};
    const int total_items = 200;

    std::thread producer([r, &enqueue_cnt]() {
        for (int i = 0; i < total_items; i++) {
            void *obj = (void *)(uint64_t)i;
            while (rte_ring_enqueue(r, obj) != 0) {
                /* spin */;
            }
            enqueue_cnt++;
        }
    });

    std::thread consumer([r, &dequeue_cnt]() {
        void *obj;
        while (dequeue_cnt < total_items) {
            if (rte_ring_dequeue(r, &obj) == 0) {
                dequeue_cnt++;
            }
        }
    });

    producer.join();
    consumer.join();

    EXPECT_EQ(enqueue_cnt, total_items);
    EXPECT_EQ(dequeue_cnt, total_items);
    EXPECT_TRUE(rte_ring_empty(r));

    rte_ring_free(r);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
```

- [ ] **Step 2: Build and run tests**

```bash
cd test && mkdir -p build && cd build
cmake ..
make
./ring_test
```

Expected: All tests pass

- [ ] **Step 3: Commit**

```bash
git add test/ring_test.cc
git commit -m "test: add rte_ring unit tests

- Single producer/consumer tests
- Multi-producer/multi-consumer tests
- Concurrent stress tests
- Wrap around and data integrity tests

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 5: Add DIF/Rate Limit/Memory Pool Tests

**Files:**
- Modify: `test/ring_test.cc` - add new test cases

- [ ] **Step 1: Add DIF tests**

Add to ring_test.cc:
```cpp
#include "test_bdev.h"

TEST(DIFTest, GuardTagCalc) {
    uint8_t data[512] = {0};
    uint16_t tag = calc_guard_tag(data, 512);
    /* Known CRC16 for zero data */
    EXPECT_EQ(tag, 0x0000);
}

TEST(DIFTest, DIFInfoRoundTrip) {
    struct io_request io = {0};
    io.buf = calloc(512, 1);
    io.lba = 100;
    io.len = 1;

    set_dif_info(&io);
    EXPECT_EQ(io.dif.ref_tag, 100u);  /* LBA lower 32 bits */
    EXPECT_NE(io.dif.guard_tag, 0);

    verify_dif_info(&io);  /* Should not crash */

    free(io.buf);
}
```

- [ ] **Step 2: Add RateLimit tests**

```cpp
TEST(RateLimitTest, Unlimited) {
    struct rate_limit limit;
    rate_limit_init(&limit, 0, 0);  /* No limits */

    EXPECT_TRUE(rate_limit_check_and_acquire(&limit));
    rate_limit_release(&limit, 4096);

    rate_limit_finish(&limit);
}

TEST(RateLimitTest, IOPSLimit) {
    struct rate_limit limit;
    rate_limit_init(&limit, 10, 0);  /* 10 IOPS */

    for (int i = 0; i < 10; i++) {
        EXPECT_TRUE(rate_limit_check_and_acquire(&limit));
    }
    /* Should be limited now */
    EXPECT_FALSE(rate_limit_check_and_acquire(&limit));

    rate_limit_finish(&limit);
}
```

- [ ] **Step 3: Add Memory Pool tests**

Note: Tests depend on test_bdev_ctx memory pool functions.

- [ ] **Step 4: Run tests**

```bash
cd test/build && ./ring_test --gtest_filter=DIFTest*:RateLimitTest*
```

- [ ] **Step 5: Commit**

```bash
git add test/ring_test.cc
git commit -m "test: add DIF and rate_limit unit tests

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 6: Integration Test

**Files:**
- Modify: `test/ring_test.cc` - integration test

- [ ] **Step 1: Add integration test**

```cpp
TEST(IntegrationTest, FullStack) {
    /* Test with standalone_test.c logic */
    struct rte_ring *r = rte_ring_create("integration", 64);
    ASSERT_NE(r, nullptr);

    /* Simulate I/O workflow */
    for (int i = 0; i < 32; i++) {
        struct io_request *io = calloc(1, sizeof(struct io_request));
        io->lba = i * 100;
        io->len = 1;
        set_dif_info(io);

        EXPECT_EQ(rte_ring_enqueue(r, io), 0);
    }

    /* Process I/O */
    uint32_t count = 0;
    while (!rte_ring_empty(r)) {
        struct io_request *io;
        if (rte_ring_dequeue(r, (void **)&io) == 0) {
            verify_dif_info(io);
            free(io);
            count++;
        }
    }

    EXPECT_EQ(count, 32);
    rte_ring_free(r);
}
```

- [ ] **Step 2: Run all tests**

```bash
cd test/build && ./ring_test
```

Expected: All tests pass

- [ ] **Step 3: Commit**

```bash
git add test/ring_test.cc
git commit -m "test: add integration test

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Summary

| Task | Files | Status |
|------|-------|-------|
| 1: Header | rte_ring.h | - [ ] |
| 2: Implementation | rte_ring.c | - [ ] |
| 3: CMake | CMakeLists.txt | - [ ] |
| 4: Unit Tests | ring_test.cc | - [ ] |
| 5: Module Tests | ring_test.cc | - [ ] |
| 6: Integration | ring_test.cc | - [ ] |