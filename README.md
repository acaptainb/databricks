# Shopping Sessions in Rust

My entry for the Databricks Shopping Sessions track. It reads the messy click logs in `/data` and writes the two daily Parquet tables to `/output`. It's one static Rust binary in an empty Docker image.

It passes all 36 fixtures and matches the 1M dev answer exactly.

## How fast

Measured on a 2 vCPU sandbox (the scorer has 8):

| Data | Time |
| --- | --- |
| 1M dev set | about 0.2 s |
| 20M (practice size) | about 3.7 s |

On 8 cores I expect around 1.5 s for practice and around 5 s for the 60M final. Peak memory at 60M should be around 10 GB plus the input files, so it stays well under 24 GB.

## How it works

Everything runs in parallel on all cores.

1. **Read.** Every CSV is memory mapped and cut into 4 MB chunks. An AVX2 scanner finds commas, newlines and quotes 64 bytes at a time. Quote and field-count rules are checked from the bit masks.
2. **Links.** Link lines are parsed in parallel and IDs are interned per hash partition. Then union-find joins them and each person is named by their smallest ID. The result goes into one flat lookup table with 64-byte slots. This runs on one thread while the other threads parse events.
3. **Events.** Each line becomes a 64-byte record (one cache line). Timestamps are parsed with SWAR tricks. Records go into 256 buckets by event_id hash using non-temporal stores.
4. **Dedup.** Each bucket gets a small hash table. The copy with the smallest ingest_ts wins, and only then is the event validated. Survivors look up their person (with prefetching) and get bucketed by person.
5. **Clicks.** Parsed the same way, at the same time as dedup. Duplicate clicks are identical, so they don't change any result and get skipped.
6. **Sessions.** Each person bucket is sorted by one 64-bit key (24 bits of person hash plus 40 bits of time). Hash collisions get fixed up after the sort. Then one pass builds sessions, picks the crediting click, and counts assists with a sliding 7-day window.
7. **Write.** Parquet files are written in parallel with the low level parquet crate.

User IDs up to 15 bytes are stored inline in the records, so comparing two IDs never touches the CSV memory. Longer IDs fall back to a pointer.

## Files

- `src/main.rs` has the pipeline.
- `src/parse.rs` has file mapping, the CSV scanner and the timestamp parser.
- `src/small.rs` has the inline 16-byte string type.
- `src/output.rs` has the Parquet writers.
- `Dockerfile` builds the static binary and copies it into an empty image.
- `fixtures/` and `scripts/` come from the starter.

## Run it

With Docker, same as the starter:

```sh
make build
make fixtures
make dev run-dev check-dev
```

Without Docker (needs Rust and `pip install polars`):

```sh
make local
```

`make local` builds with cargo, runs the fixtures, and checks the dev set if `dev/` and `dev-answer/` are there.

Set `SS_VERBOSE=1` to print how long each step takes to stderr.
