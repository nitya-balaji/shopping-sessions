# Shopping Sessions: native C++ pipeline.
# Build stage compiles one static binary; the run image holds nothing else, so container start is minimal.
# Scorers are Intel Xeon (Cascade Lake or newer): x86-64-v3 (AVX2) is safe there.
FROM gcc:13-bookworm AS build
WORKDIR /src
COPY src/ src/
RUN g++ -std=c++20 -O3 -march=x86-64-v3 -mtune=cascadelake -pthread -static \
      -o /pipeline src/main.cpp

FROM scratch
COPY --from=build /pipeline /pipeline
# Runs as uid 1000 with a read-only root: reads $DATA_DIR, writes $OUTPUT_DIR, uses $NCPU threads.
# malloc-backed buffers on 2 MB pages too (ignored where unsupported)
ENV GLIBC_TUNABLES=glibc.malloc.hugetlb=1
USER 1000:1000
CMD ["/pipeline"]
