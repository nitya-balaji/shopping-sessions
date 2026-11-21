#!/bin/sh
# Build the pipeline: ./build.sh [extra g++ flags]. Output: build/pipeline
set -e
cd "$(dirname "$0")"
mkdir -p build
g++ -std=c++20 -O3 -march=x86-64-v3 -pthread -Wall -Wno-unused-result "$@" -o build/pipeline src/main.cpp
