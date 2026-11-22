# Shopping Sessions: Rust pipeline.
# Stage 1 builds a fully static release binary; stage 2 is an empty image
# holding only that binary.
FROM rust:1-slim AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src src
COPY pgo/generator /gen
# AVX2-era baseline with Skylake scheduling (scorers are Cascade Lake) and
# static linking; branches kept off 32-byte boundaries (Cascade Lake JCC
# erratum microcode penalty). An explicit --target keeps these flags off build
# scripts and proc-macros.
ENV BASEFLAGS="-C target-cpu=skylake -C target-feature=+crt-static -C llvm-args=-x86-branches-within-32B-boundaries"

# 1) Plain optimized build. This alone is a complete, correct binary.
RUN RUSTFLAGS="$BASEFLAGS" cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/shopping-sessions /shopping-sessions

# 2) Profile-guided optimization on top, best effort: the published generator
#    (pgo/generator) makes a 2M-event training set, an instrumented build runs
#    on it, and the program is rebuilt with the merged profile. The PGO binary
#    replaces the plain one only if every step worked and it ran cleanly on
#    the training set; any failure keeps the plain build. PGO changes code
#    layout, never behavior.
RUN ( rustup component add llvm-tools-preview \
   && (cd /gen && cargo build --release --locked) \
   && /gen/target/release/generate --seed 3 --events 2000000 --clicks 500000 --output /train \
   && RUSTFLAGS="$BASEFLAGS -C profile-generate=/pgo" cargo build --release --locked --target x86_64-unknown-linux-gnu --target-dir /t-gen \
   && DATA_DIR=/train OUTPUT_DIR=/tmp/train-out NCPU=4 /t-gen/x86_64-unknown-linux-gnu/release/shopping-sessions \
   && "$(find / -name llvm-profdata -type f 2>/dev/null | head -1)" merge -o /pgo.profdata /pgo \
   && RUSTFLAGS="$BASEFLAGS -C profile-use=/pgo.profdata" cargo build --release --locked --target x86_64-unknown-linux-gnu --target-dir /t-use \
   && DATA_DIR=/train OUTPUT_DIR=/tmp/check-out NCPU=4 /t-use/x86_64-unknown-linux-gnu/release/shopping-sessions \
   && test -s /tmp/check-out/user_daily/part-00000.parquet \
   && cp /t-use/x86_64-unknown-linux-gnu/release/shopping-sessions /shopping-sessions \
   && echo "PGO: applied" ) || echo "PGO: skipped, keeping the plain build"; \
   (strip /shopping-sessions || true)

FROM scratch
COPY --from=build /shopping-sessions /shopping-sessions
# The scorer sets DATA_DIR, OUTPUT_DIR, NCPU and runs as uid 1000.
# Huge pages for the allocator (fewer page faults and TLB misses) and keep
# freed memory for reuse instead of returning it to the kernel.
ENV MIMALLOC_ALLOW_LARGE_OS_PAGES=1 MIMALLOC_PURGE_DELAY=-1
CMD ["/shopping-sessions"]
