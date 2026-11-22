# Shopping Sessions: Rust pipeline.
# Stage 1 builds a fully static release binary. Stage 2 is an empty image
# holding only that binary, so there is nothing to start but the program.
FROM rust:1-slim AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src src
# AVX2 baseline (scorers are Cascade Lake or newer) and static linking.
# An explicit --target keeps these flags off build scripts and proc-macros.
# Scorers are Cascade Lake: keep branches off 32-byte boundaries (JCC erratum
# microcode penalty), as GCC's -Wa,-mbranches-within-32B-boundaries does.
ENV RUSTFLAGS="-C target-cpu=skylake -C target-feature=+crt-static -C llvm-args=-x86-branches-within-32B-boundaries"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/shopping-sessions /shopping-sessions \
 && (strip /shopping-sessions || true)

FROM scratch
COPY --from=build /shopping-sessions /shopping-sessions
# The scorer sets DATA_DIR, OUTPUT_DIR, NCPU and runs as uid 1000.
# Huge pages for the allocator (fewer page faults and TLB misses) and keep
# freed memory for reuse instead of returning it to the kernel.
ENV MIMALLOC_ALLOW_LARGE_OS_PAGES=1 MIMALLOC_PURGE_DELAY=-1
CMD ["/shopping-sessions"]
