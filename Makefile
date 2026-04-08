# SPDX-License-Identifier: BSD-3-Clause
# Makefile for Custom Bdev Module (Standalone - No SPDK dependency)

# Directories
ROOT_DIR := $(shell pwd)
LIB_DIR := $(ROOT_DIR)/lib/test_bdev
LIB_IOPERF_DIR := $(ROOT_DIR)/lib/ioperf
TEST_DIR := $(ROOT_DIR)/test
INC_DIR := $(ROOT_DIR)/include
TOOLS_DIR := $(ROOT_DIR)/tools

# Build targets
TARGET := test_bdev_test
STANDALONE_TARGET := test_bdev_standalone
BENCHMARK_TARGET := ioperf_benchmark
OBJS := $(LIB_DIR)/test_bdev.o
TEST_SRC := $(TEST_DIR)/standalone_test.c

# Compiler and flags
CC ?= gcc
CFLAGS := -Wall -Wextra -g -O2 -fPIC -pthread -D_GNU_SOURCE
CFLAGS += -I$(INC_DIR) -I$(LIB_DIR) -I$(LIB_IOPERF_DIR)
LDFLAGS := -lpthread

.PHONY: all clean standalone benchmark help

all: standalone

standalone: $(TEST_SRC)
	@echo "Building standalone test version..."
	$(CC) $(CFLAGS) -o $(ROOT_DIR)/$(STANDALONE_TARGET) $< $(LDFLAGS)
	@echo "Build complete: $(ROOT_DIR)/$(STANDALONE_TARGET)"

benchmark: $(LIB_IOPERF_DIR)/ioperf.c $(TOOLS_DIR)/ioperf_benchmark.c
	@echo "Building ioperf benchmark..."
	$(CC) $(CFLAGS) -o $(ROOT_DIR)/$(BENCHMARK_TARGET) $(TOOLS_DIR)/ioperf_benchmark.c $(LIB_IOPERF_DIR)/ioperf.c $(LDFLAGS)
	@echo "Build complete: $(ROOT_DIR)/$(BENCHMARK_TARGET)"

$(LIB_DIR)/test_bdev.o: $(LIB_DIR)/test_bdev.c $(LIB_DIR)/test_bdev.h
	@mkdir -p $(ROOT_DIR)/build
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f $(ROOT_DIR)/$(TARGET)
	rm -f $(ROOT_DIR)/$(STANDALONE_TARGET)
	rm -f $(ROOT_DIR)/$(BENCHMARK_TARGET)
	rm -f $(ROOT_DIR)/build/*
	rm -f $(OBJS)

# Help target
help:
	@echo "Custom Bdev Module Build System (Standalone - No SPDK)"
	@echo ""
	@echo "Targets:"
	@echo "  standalone - Build standalone test version (no SPDK required)"
	@echo "  benchmark  - Build ioperf benchmark tool"
	@echo "  all        - Alias for standalone"
	@echo "  clean      - Remove build artifacts"
	@echo "  help       - Show this help message"
	@echo ""
	@echo "Benchmark usage:"
	@echo "  make benchmark"
	@echo "  ./ioperf_benchmark --read -t 4 -T 10"
	@echo "  ./ioperf_benchmark --write -t 4 -T 10"
	@echo "  ./ioperf_benchmark --rand -t 8 -T 30"
	@echo "  ./ioperf_benchmark --read -r 100 -w 200 -t 4 -T 10"
