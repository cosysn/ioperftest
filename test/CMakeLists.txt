cmake_minimum_required(VERSION 3.16)
project(ring_test CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)

FetchContent_MakeAvailable(googletest)

enable_testing()

include_directories(
    ${CMAKE_SOURCE_DIR}/lib/test_bdev
    ${CMAKE_SOURCE_DIR}/include
)

add_executable(ring_test
    ring_test.cc
)

target_link_libraries(ring_test
    gtest_main
    gtest
    Threads::Threads
)

include(GoogleTest)
gtest_discover_tests(ring_test)
