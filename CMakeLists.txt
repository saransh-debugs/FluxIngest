cmake_minimum_required(VERSION 3.25)
project(fluxingest LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

option(FLUX_BUILD_TESTS "Build GoogleTest tests" ON)
option(FLUX_BUILD_BENCH "Build Google Benchmark micro-benchmarks" ON)
option(FLUX_BUILD_FUZZ "Build libFuzzer target (needs LLVM clang)" OFF)
set(FLUX_SANITIZE "" CACHE STRING "Sanitizer set: '', 'address', 'thread'")

# Arrow/Parquet are optional. Without them we still build and test the whole
# pipeline (ring, interner, parser, consumer, writer) against fake sinks.
# Homebrew has no Arrow bottle for Intel Macs, so the Parquet half is built in
# docker/Dockerfile.
find_package(Arrow CONFIG QUIET)
find_package(Parquet CONFIG QUIET)
if(Arrow_FOUND AND Parquet_FOUND)
  set(FLUX_WITH_ARROW ON)
  message(STATUS "FluxIngest: Arrow ${Arrow_VERSION} found, building Parquet sink")
else()
  set(FLUX_WITH_ARROW OFF)
  message(STATUS "FluxIngest: Arrow not found, building core only")
endif()

include(FetchContent)
set(FETCHCONTENT_QUIET ON)

# fast_float: the same algorithm libstdc++ uses for std::from_chars(double).
# Apple's libc++ doesn't ship floating-point from_chars yet, and strtod needs a
# NUL-terminated string plus a locale lookup on every call.
FetchContent_Declare(fast_float
  URL https://github.com/fastfloat/fast_float/archive/refs/tags/v8.0.2.tar.gz
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE SYSTEM)
FetchContent_MakeAvailable(fast_float)

set(FLUX_WARNINGS -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion
                  -Wnon-virtual-dtor -Wold-style-cast -Werror)

if(FLUX_SANITIZE STREQUAL "address")
  add_compile_options(-fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
elseif(FLUX_SANITIZE STREQUAL "thread")
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
  add_link_options(-fsanitize=thread)
endif()

# ---- core: everything except Arrow ----
set(FLUX_CORE_SOURCES src/line_protocol.cpp src/batch.cpp src/writer.cpp src/udp_receiver.cpp src/pipeline.cpp)
add_library(flux_core STATIC ${FLUX_CORE_SOURCES})
target_include_directories(flux_core PUBLIC include src)
target_link_libraries(flux_core PUBLIC FastFloat::fast_float)
target_compile_options(flux_core PRIVATE ${FLUX_WARNINGS})
find_package(Threads REQUIRED)
target_link_libraries(flux_core PUBLIC Threads::Threads)

if(FLUX_WITH_ARROW)
  add_library(flux_parquet STATIC src/parquet_sink.cpp)
  target_link_libraries(flux_parquet PUBLIC flux_core Parquet::parquet_shared Arrow::arrow_shared)
  target_compile_options(flux_parquet PRIVATE ${FLUX_WARNINGS})
  target_compile_definitions(flux_parquet PUBLIC FLUX_WITH_ARROW=1)

  add_executable(fluxingest src/main.cpp)
  target_link_libraries(fluxingest PRIVATE flux_parquet)
  target_compile_options(fluxingest PRIVATE ${FLUX_WARNINGS})

endif()


add_executable(flux_loadgen tools/loadgen.cpp)
target_link_libraries(flux_loadgen PRIVATE flux_core)
target_compile_options(flux_loadgen PRIVATE ${FLUX_WARNINGS})

# ---- tests ----
if(FLUX_BUILD_TESTS)
  FetchContent_Declare(googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE SYSTEM)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
  enable_testing()
  include(GoogleTest)

  set(FLUX_TESTS
    test_spsc_ring test_ring_stress test_interner test_line_protocol
    test_ingestor test_consumer_writer test_pipeline)
  foreach(t ${FLUX_TESTS})
    add_executable(${t} tests/${t}.cpp)
    target_link_libraries(${t} PRIVATE flux_core GTest::gtest_main)
    gtest_discover_tests(${t} DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 600)
  endforeach()

  if(FLUX_WITH_ARROW)
    add_executable(test_parquet_sink tests/test_parquet_sink.cpp)
    target_link_libraries(test_parquet_sink PRIVATE flux_parquet GTest::gtest_main)
    gtest_discover_tests(test_parquet_sink DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 600)
  endif()

  # Replaces global operator new to count allocations. ASan and TSan want to
  # own operator new themselves, so this test only exists in plain builds.
  if(FLUX_SANITIZE STREQUAL "")
    add_executable(test_no_alloc tests/test_no_alloc.cpp)
    target_link_libraries(test_no_alloc PRIVATE flux_core GTest::gtest_main)
    gtest_discover_tests(test_no_alloc DISCOVERY_TIMEOUT 60)
  endif()
endif()

# ---- benchmarks ----
if(FLUX_BUILD_BENCH AND FLUX_SANITIZE STREQUAL "")
  FetchContent_Declare(benchmark
    URL https://github.com/google/benchmark/archive/refs/tags/v1.9.1.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE SYSTEM)
  set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
  set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(benchmark)
  foreach(b bench_ring bench_parse)
    add_executable(${b} bench/${b}.cpp)
    target_link_libraries(${b} PRIVATE flux_core benchmark::benchmark)
  endforeach()
  add_executable(bench_pipeline bench/bench_pipeline.cpp)
  if(FLUX_WITH_ARROW)
    target_link_libraries(bench_pipeline PRIVATE flux_parquet)
  else()
    target_link_libraries(bench_pipeline PRIVATE flux_core)
  endif()
endif()

# ---- fuzz ----
# The parser source is compiled into the target (not linked from flux_core)
# so libFuzzer's coverage instrumentation sees inside it.
if(FLUX_BUILD_FUZZ)
  add_executable(fuzz_line_protocol fuzz/fuzz_line_protocol.cpp src/line_protocol.cpp)
  target_include_directories(fuzz_line_protocol PRIVATE include)
  target_link_libraries(fuzz_line_protocol PRIVATE FastFloat::fast_float)
  target_compile_options(fuzz_line_protocol PRIVATE -fsanitize=fuzzer,address,undefined -fno-sanitize-recover=all)
  target_link_options(fuzz_line_protocol PRIVATE -fsanitize=fuzzer,address,undefined)
endif()
