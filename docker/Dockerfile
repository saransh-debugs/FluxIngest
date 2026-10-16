# Build + test image for the Arrow/Parquet half of FluxIngest.
# Homebrew has no Arrow bottle for Intel Macs, so the full build lives here.
FROM ubuntu:24.04

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates lsb-release wget gnupg \
        build-essential cmake ninja-build git clang llvm python3 python3-pip \
    && wget -q https://packages.apache.org/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-noble.deb \
    && apt-get install -y ./apache-arrow-apt-source-latest-noble.deb \
    && rm apache-arrow-apt-source-latest-noble.deb \
    && apt-get update && apt-get install -y --no-install-recommends \
        libarrow-dev libparquet-dev \
    && pip3 install --break-system-packages --no-cache-dir duckdb \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
