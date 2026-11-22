// Parquet writers for the two output tables (low-level parquet API, no Arrow).

use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use crate::UserRow;

type S = &'static [u8];

pub struct CampRow {
    pub id: S,
    pub channel: S,
    pub day: i32,
    pub sessions: u64,
    pub purchasing: u64,
    pub revenue: u64,
    pub assisted: u64,
}

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .build(),
    )
}

/// Zero-copy: every string written is a static slice of a mapped input file.
fn ba(b: &'static [u8]) -> ByteArray {
    ByteArray::from(bytes::Bytes::from_static(b))
}

pub fn write_user(path: &Path, rows: &[UserRow], cats: &[S]) {
    let schema = Arc::new(
        parse_message_type(
            "message user_daily {
                required binary user_id (UTF8);
                required int32 day (DATE);
                required int64 sessions;
                required int64 events;
                required int64 revenue_cents;
                optional binary top_category (UTF8);
            }",
        )
        .unwrap(),
    );
    let file = BufWriter::with_capacity(1 << 20, File::create(path).expect("create parquet"));
    let mut w = SerializedFileWriter::new(file, schema, props()).unwrap();
    const RG: usize = 1 << 20;
    for chunk in rows.chunks(RG.max(1)) {
        let mut rg = w.next_row_group().unwrap();
        let mut ci = 0;
        while let Some(mut col) = rg.next_column().unwrap() {
            match ci {
                0 => {
                    let v: Vec<ByteArray> = chunk.iter().map(|r| ba(r.name.as_bytes())).collect();
                    col.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
                }
                1 => {
                    let v: Vec<i32> = chunk.iter().map(|r| r.day).collect();
                    col.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
                }
                2 => {
                    let v: Vec<i64> = chunk.iter().map(|r| r.sessions as i64).collect();
                    col.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
                }
                3 => {
                    let v: Vec<i64> = chunk.iter().map(|r| r.events as i64).collect();
                    col.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
                }
                4 => {
                    let v: Vec<i64> = chunk.iter().map(|r| r.revenue as i64).collect();
                    col.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
                }
                5 => {
                    let mut vals: Vec<ByteArray> = Vec::new();
                    let mut defs: Vec<i16> = Vec::with_capacity(chunk.len());
                    for r in chunk {
                        if r.top >= 0 {
                            vals.push(ba(cats[r.top as usize]));
                            defs.push(1);
                        } else {
                            defs.push(0);
                        }
                    }
                    col.typed::<ByteArrayType>().write_batch(&vals, Some(&defs), None).unwrap();
                }
                _ => unreachable!(),
            }
            col.close().unwrap();
            ci += 1;
        }
        rg.close().unwrap();
    }
    w.close().unwrap();
}

pub fn write_campaign(path: &Path, rows: &[CampRow]) {
    let schema = Arc::new(
        parse_message_type(
            "message campaign_daily {
                required binary campaign_id (UTF8);
                required binary channel (UTF8);
                required int32 day (DATE);
                required int64 sessions;
                required int64 purchasing_sessions;
                required int64 revenue_cents;
                required int64 assisted_sessions;
            }",
        )
        .unwrap(),
    );
    let file = BufWriter::new(File::create(path).expect("create parquet"));
    let mut w = SerializedFileWriter::new(file, schema, props()).unwrap();
    let mut rg = w.next_row_group().unwrap();
    let mut ci = 0;
    while let Some(mut col) = rg.next_column().unwrap() {
        match ci {
            0 | 1 => {
                let v: Vec<ByteArray> =
                    rows.iter().map(|r| ba(if ci == 0 { r.id } else { r.channel })).collect();
                col.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
            }
            2 => {
                let v: Vec<i32> = rows.iter().map(|r| r.day).collect();
                col.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
            }
            _ => {
                let v: Vec<i64> = rows
                    .iter()
                    .map(|r| match ci {
                        3 => r.sessions,
                        4 => r.purchasing,
                        5 => r.revenue,
                        _ => r.assisted,
                    } as i64)
                    .collect();
                col.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
            }
        }
        col.close().unwrap();
        ci += 1;
    }
    rg.close().unwrap();
    w.close().unwrap();
}
