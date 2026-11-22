# Shopping Sessions: Rust pipeline.
# Stage 1 builds a fully static release binary with profile-guided
# optimization. Stage 2 is an empty image holding only that binary.
FROM rust:1-slim AS build
RUN rustup component add llvm-tools-preview

# PGO training data: the published dataset generator (source in pgo/generator)
# makes a small 2M-event set inside the build. Built outside /src so the
# project's .cargo settings do not apply to it.
COPY pgo/generator /gen
RUN cd /gen && cargo build --release --locked \
 && /gen/target/release/generate --seed 3 --events 2000000 --clicks 500000 --output /train

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src src
# AVX2-era baseline with Skylake scheduling (scorers are Cascade Lake) and
# static linking; branches kept off 32-byte boundaries (Cascade Lake JCC
# erratum microcode penalty). An explicit --target keeps these flags off build
# scripts and proc-macros.
ENV BASEFLAGS="-C target-cpu=skylake -C target-feature=+crt-static -C llvm-args=-x86-branches-within-32B-boundaries"
# 1) instrumented build, 2) one run on the training set, 3) merge the profile,
# 4) optimized build using it. PGO changes code layout, never behavior.
RUN RUSTFLAGS="$BASEFLAGS -C profile-generate=/pgo" cargo build --release --locked --target x86_64-unknown-linux-gnu --target-dir /t-gen \
 && DATA_DIR=/train OUTPUT_DIR=/tmp/train-out NCPU=4 /t-gen/x86_64-unknown-linux-gnu/release/shopping-sessions \
 && "$(find / -name llvm-profdata -type f 2>/dev/null | head -1)" merge -o /pgo.profdata /pgo \
 && RUSTFLAGS="$BASEFLAGS -C profile-use=/pgo.profdata" cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/shopping-sessions /shopping-sessions \
 && (strip /shopping-sessions || true)

FROM scratch
COPY --from=build /shopping-sessions /shopping-sessions
# The scorer sets DATA_DIR, OUTPUT_DIR, NCPU and runs as uid 1000.
# Huge pages for the allocator (fewer page faults and TLB misses) and keep
# freed memory for reuse instead of returning it to the kernel.
ENV MIMALLOC_ALLOW_LARGE_OS_PAGES=1 MIMALLOC_PURGE_DELAY=-1
CMD ["/shopping-sessions"]
