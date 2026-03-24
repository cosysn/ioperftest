# SPDX-License-Identifier: BSD-3-Clause
# Makefile for Custom Bdev Module (Standalone - No SPDK dependency)

# Directories
ROOT_DIR := $(shell pwd)
LIB_DIR := $(ROOT_DIR)/lib/test_bdev
TEST_DIR := $(ROOT_DIR)/test
INC_DIR := $(ROOT_DIR)/include

# Build targets
TARGET := test_bdev_test
STANDALONE_TARGET := test_bdev_standalone
OBJS := $(LIB_DIR)/test_bdev.o
TEST_SRC := $(TEST_DIR)/standalone_test.c

# Compiler and flags
CC ?= gcc
CFLAGS := -Wall -Wextra -g -O2 -fPIC -pthread
CFLAGS += -I$(INC_DIR) -I$(LIB_DIR)
LDFLAGS := -lpthread

.PHONY: all clean standalone help

all: standalone

standalone: $(TEST_SRC)
	@echo "Building standalone test version..."
	$(CC) $(CFLAGS) -o $(ROOT_DIR)/$(STANDALONE_TARGET) $< $(LDFLAGS)
	@echo "Build complete: $(ROOT_DIR)/$(STANDALONE_TARGET)"

$(LIB_DIR)/test_bdev.o: $(LIB_DIR)/test_bdev.c $(LIB_DIR)/test_bdev.h
	@mkdir -p $(ROOT_DIR)/build
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f $(ROOT_DIR)/$(TARGET)
	rm -f $(ROOT_DIR)/$(STANDALONE_TARGET)
	rm -f $(ROOT_DIR)/build/*
	rm -f $(OBJS)

# Help target
help:
	@echo "Custom Bdev Module Build System (Standalone - No SPDK)"
	@echo ""
	@echo "Targets:"
	@echo "  standalone - Build standalone test version (no SPDK required)"
	@echo "  all        - Alias for standalone"
	@echo "  clean      - Remove build artifacts"
	@echo "  help       - Show this help message"
	@echo ""
	@echo "Usage:"
	@echo "  make"
	@echo "  ./test_bdev_standalone --max-iops=100000 --max-bw=500"
