# Shopping Sessions: Rust pipeline.
# Stage 1 builds a fully static release binary. Stage 2 is an empty image
# holding only that binary, so there is nothing to start but the program.
FROM rust:1-slim AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src src
# AVX2 baseline (scorers are Cascade Lake or newer) and static linking.
# An explicit --target keeps these flags off build scripts and proc-macros.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/shopping-sessions /shopping-sessions \
 && (strip /shopping-sessions || true)

FROM scratch
COPY --from=build /shopping-sessions /shopping-sessions
# The scorer sets DATA_DIR, OUTPUT_DIR, NCPU and runs as uid 1000.
CMD ["/shopping-sessions"]
