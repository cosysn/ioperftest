# SPDX-License-Identifier: BSD-3-Clause
# Makefile for Custom SPDK Bdev Module

# Directories
ROOT_DIR := $(shell pwd)
SPDKBUILD ?= /opt/spdk
LIB_DIR := $(ROOT_DIR)/lib/custom_bdev
TEST_DIR := $(ROOT_DIR)/test
INC_DIR := $(ROOT_DIR)/include

# Build targets
TARGET := custom_bdev_test
STANDALONE_TARGET := custom_bdev_standalone
OBJS := $(LIB_DIR)/custom_bdev.o
TEST_SRC := $(TEST_DIR)/standalone_test.c

# Compiler and flags
CC ?= gcc
CFLAGS := -Wall -Wextra -g -O2 -fPIC -pthread
CFLAGS += -I$(INC_DIR)
LDFLAGS := -lpthread

# SPDK-specific flags
SPDKBUILD ?= /opt/spdk
SPDKBUILD_FLAGS := -I$(SPDKBUILD)/include -I$(SPDKBUILD)/build/include

# Check if SPDK is built
SPDK_AVAILABLE := $(shell if [ -f "$(SPDKBUILD)/build/lib/libspdk.so" ]; then echo "true"; else echo "false"; fi)

.PHONY: all clean standalone check_spdk

all: standalone

standalone: $(TEST_SRC)
	@echo "Building standalone test version..."
	$(CC) $(CFLAGS) -o $(ROOT_DIR)/$(STANDALONE_TARGET) $< $(LDFLAGS)
	@echo "Build complete: $(ROOT_DIR)/$(STANDALONE_TARGET)"

check_spdk:
	@echo "Checking for SPDK..."
	@if [ -f "$(SPDKBUILD)/build/lib/libspdk.so" ]; then \
		echo "SPDK found at $(SPDKBUILD)"; \
	else \
		echo "SPDK not found at $(SPDKBUILD). Building standalone version."; \
	fi

$(LIB_DIR)/custom_bdev.o: $(LIB_DIR)/custom_bdev.c $(LIB_DIR)/custom_bdev.h
	@mkdir -p $(ROOT_DIR)/build
	$(CC) $(CFLAGS) $(SPDKBUILD_FLAGS) -c -o $@ $<

clean:
	rm -f $(ROOT_DIR)/$(TARGET)
	rm -f $(ROOT_DIR)/$(STANDALONE_TARGET)
	rm -f $(ROOT_DIR)/build/*
	rm -f $(OBJS)

# Help target
help:
	@echo "Custom SPDK Bdev Module Build System"
	@echo ""
	@echo "Targets:"
	@echo "  standalone - Build standalone test version (no SPDK required)"
	@echo "  all        - Alias for standalone"
	@echo "  clean      - Remove build artifacts"
	@echo "  help       - Show this help message"
	@echo ""
	@echo "Options:"
	@echo "  SPDKBUILD=/path/to/spdk - Specify SPDK installation directory"
	@echo ""
	@echo "Usage:"
	@echo "  make"
	@echo "  ./custom_bdev_standalone --max-iops=100000 --max-bw=500"
