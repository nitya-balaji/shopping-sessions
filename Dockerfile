# Shopping Sessions: native C++ pipeline, built with profile-guided optimization.
# Stage 1 builds the track's generator (no external crates) and makes a 1M-event training set.
# Stage 2 compiles an instrumented binary, runs it once on that data, and recompiles with the profile.
# If any PGO step fails, it falls back to the plain optimized build, so the image always builds.
# Scorers are Intel Xeon (Cascade Lake or newer): x86-64-v3 (AVX2) is safe there.
FROM rust:1.85-slim-bookworm AS gen
WORKDIR /g
COPY generator/ ./
RUN cargo build --release \
 && ./target/release/generate --seed 4242 --events 1000000 --clicks 250000 --output /train

FROM gcc:13-bookworm AS build
WORKDIR /src
COPY src/ src/
COPY --from=gen /train /train
ARG CXXFLAGS="-std=c++20 -O3 -march=x86-64-v3 -mtune=cascadelake -pthread -static"
RUN set -e; \
    if g++ $CXXFLAGS -fprofile-generate -fprofile-update=atomic -fprofile-dir=/pgo -o /pipeline src/main.cpp src/pgo_gen.cpp \
       && mkdir -p /tmp/train-out && NCPU=4 /pipeline /train /tmp/train-out \
       && g++ $CXXFLAGS -fprofile-use -fprofile-dir=/pgo -fprofile-partial-training -Wno-missing-profile -o /pipeline src/main.cpp; \
    then echo "built with PGO"; \
    else echo "PGO failed, plain build"; g++ $CXXFLAGS -o /pipeline src/main.cpp; fi

FROM scratch
COPY --from=build /pipeline /pipeline
# Runs as uid 1000 with a read-only root: reads $DATA_DIR, writes $OUTPUT_DIR, uses $NCPU threads.
# malloc-backed buffers on 2 MB pages too (ignored where unsupported)
ENV GLIBC_TUNABLES=glibc.malloc.hugetlb=1
USER 1000:1000
CMD ["/pipeline"]
