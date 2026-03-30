#include <gtest/gtest.h>

extern "C" {
#include "rte_ring.h"
}

class RingTest : public ::testing::Test {
protected:
    struct rte_ring *ring = nullptr;

    void SetUp() override {
        ring = rte_ring_create("test_ring", 8);
        ASSERT_NE(ring, nullptr);
    }

    void TearDown() override {
        if (ring) {
            rte_ring_free(ring);
            ring = nullptr;
        }
    }
};

TEST_F(RingTest, CreateAndFree) {
    EXPECT_STREQ(rte_ring_get_name(ring), "test_ring");
    EXPECT_EQ(ring->capacity, 8);
}

TEST_F(RingTest, InitiallyEmpty) {
    EXPECT_TRUE(rte_ring_empty(ring));
    EXPECT_FALSE(rte_ring_full(ring));
    EXPECT_EQ(rte_ring_count(ring), 0);
}

TEST_F(RingTest, EnqueueDequeue) {
    int value = 42;
    EXPECT_EQ(rte_ring_enqueue(ring, &value), 0);
    EXPECT_EQ(rte_ring_count(ring), 1);

    void *out = nullptr;
    EXPECT_EQ(rte_ring_dequeue(ring, &out), 0);
    EXPECT_EQ(out, &value);
}

TEST_F(RingTest, EnqueueDequeueMultiple) {
    int values[8] = {1, 2, 3, 4, 5, 6, 7, 8};

    for (int i = 0; i < 8; i++) {
        EXPECT_EQ(rte_ring_enqueue(ring, &values[i]), 0);
    }

    EXPECT_TRUE(rte_ring_full(ring));
    EXPECT_EQ(rte_ring_free_count(ring), 0);

    for (int i = 0; i < 8; i++) {
        void *out = nullptr;
        EXPECT_EQ(rte_ring_dequeue(ring, &out), 0);
        EXPECT_EQ(out, &values[i]);
    }

    EXPECT_TRUE(rte_ring_empty(ring));
}

TEST_F(RingTest, EnqueueBeyondCapacity) {
    int value = 1;

    for (int i = 0; i < 8; i++) {
        EXPECT_EQ(rte_ring_enqueue(ring, &value), 0);
    }

    // Should fail when ring is full
    EXPECT_EQ(rte_ring_enqueue(ring, &value), -1);
}

TEST(RingTest, NullName) {
    struct rte_ring *r = rte_ring_create(nullptr, 4);
    EXPECT_NE(r, nullptr);
    rte_ring_free(r);
}
