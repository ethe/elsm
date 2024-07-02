use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use arrow::{
    array::{
        Array, StringArray, StringBuilder, StructArray, StructBuilder, UInt32Array, UInt32Builder,
    },
    datatypes::{DataType, Field, Fields, SchemaRef},
    record_batch::RecordBatch,
};
use criterion::{criterion_group, criterion_main, Criterion};
use elsm::{
    oracle::LocalOracle,
    record::RecordType,
    schema::{Builder, Schema},
    serdes::{Decode, Encode},
    wal::provider::in_mem::InMemProvider,
    Db, DbOption,
};
use elsm_marco::elsm_schema;
use executor::{
    futures::{future::block_on, io},
    ExecutorBuilder,
};
use itertools::Itertools;
use lazy_static::lazy_static;

fn counter() -> usize {
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

    static C: AtomicUsize = AtomicUsize::new(0);

    C.fetch_add(1, Relaxed)
}

#[derive(Debug, Eq, PartialEq)]
#[elsm_schema]
pub(crate) struct TestString {
    #[primary_key]
    pub(crate) string_0: String,
    pub(crate) string_1: String,
}

#[derive(Debug, Eq, PartialEq)]
#[elsm_schema]
pub(crate) struct User {
    #[primary_key]
    pub(crate) id: u32,
    pub(crate) i_number_0: u32,
}

fn random(n: u32) -> u32 {
    use std::{cell::Cell, num::Wrapping};

    thread_local! {
        static RNG: Cell<Wrapping<u32>> = Cell::new(Wrapping(1406868647));
    }

    RNG.with(|rng| {
        let mut x = rng.get();
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        rng.set(x);

        ((x.0 as u64).wrapping_mul(n as u64) >> 32) as u32
    })
}

fn elsm_bulk_load(c: &mut Criterion) {
    let count = AtomicU32::new(0_u32);
    let bytes = |len| -> String {
        String::from_utf8(
            count
                .fetch_add(1, Ordering::Relaxed)
                .to_be_bytes()
                .into_iter()
                .cycle()
                .take(len)
                .collect_vec(),
        )
        .unwrap()
    };

    let mut bench = |key_len, val_len| {
        let db = block_on(async {
            Db::new(
                LocalOracle::default(),
                InMemProvider::default(),
                DbOption::new(format!("monotonic_crud/{}/", counter())),
            )
            .await
            .unwrap()
        });

        c.bench_function(
            &format!("bulk load key/value lengths {}/{}", key_len, val_len),
            |b| {
                b.to_async(ExecutorBuilder::new().build().unwrap())
                    .iter(|| async {
                        db.write(
                            RecordType::Full,
                            0,
                            TestStringInner::new(bytes(key_len), bytes(val_len)),
                        )
                        .await
                        .unwrap();
                    })
            },
        );
    };

    for key_len in &[10_usize, 128, 256, 512] {
        for val_len in &[0_usize, 10, 128, 256, 512, 1024, 2048, 4096, 8192] {
            bench(*key_len, *val_len)
        }
    }
}

fn elsm_monotonic_crud(c: &mut Criterion) {
    let db = block_on(async {
        Db::new(
            LocalOracle::default(),
            InMemProvider::default(),
            DbOption::new(format!("monotonic_crud/{}/", counter())),
        )
        .await
        .unwrap()
    });

    c.bench_function("monotonic inserts", |b| {
        let count = AtomicU32::new(0_u32);
        b.to_async(ExecutorBuilder::new().build().unwrap())
            .iter(|| async {
                let count = count.fetch_add(1, Ordering::Relaxed);
                db.write(RecordType::Full, 0, UserInner::new(count, count))
                    .await
                    .unwrap();
            })
    });

    c.bench_function("monotonic gets", |b| {
        let count = AtomicU32::new(0_u32);
        b.to_async(ExecutorBuilder::new().build().unwrap())
            .iter(|| async {
                let count = count.fetch_add(1, Ordering::Relaxed);
                db.get(&count, &0).await.unwrap();
            })
    });

    c.bench_function("monotonic removals", |b| {
        let count = AtomicU32::new(0_u32);
        b.to_async(ExecutorBuilder::new().build().unwrap())
            .iter(|| async {
                let count = count.fetch_add(1, Ordering::Relaxed);
                db.remove(RecordType::Full, 0, count).await.unwrap();
            })
    });
}

fn elsm_random_crud(c: &mut Criterion) {
    const SIZE: u32 = 65536;

    let db = block_on(async {
        Db::new(
            LocalOracle::default(),
            InMemProvider::default(),
            DbOption::new(format!("random_crud/{}/", counter())),
        )
        .await
        .unwrap()
    });

    c.bench_function("random inserts", |b| {
        b.to_async(ExecutorBuilder::new().build().unwrap())
            .iter(|| async {
                db.write(
                    RecordType::Full,
                    0,
                    UserInner::new(random(SIZE), random(SIZE)),
                )
                .await
                .unwrap();
            })
    });

    c.bench_function("random gets", |b| {
        b.to_async(ExecutorBuilder::new().build().unwrap())
            .iter(|| async {
                db.get(&random(SIZE), &0).await.unwrap();
            })
    });

    c.bench_function("random removals", |b| {
        b.to_async(ExecutorBuilder::new().build().unwrap())
            .iter(|| async {
                db.remove(RecordType::Full, 0, random(SIZE)).await.unwrap();
            })
    });
}

fn elsm_empty_opens(c: &mut Criterion) {
    let _ = std::fs::remove_dir_all("empty_opens");

    c.bench_function("empty opens", |b| {
        b.to_async(ExecutorBuilder::new().build().unwrap())
            .iter(|| async {
                Db::<UserInner, LocalOracle<<UserInner as Schema>::PrimaryKey>, InMemProvider>::new(
                    LocalOracle::default(),
                    InMemProvider::default(),
                    DbOption::new(format!("empty_opens/{}/", counter())),
                )
                .await
                .unwrap()
            })
    });
    let _ = std::fs::remove_dir_all("empty_opens");
}

criterion_group!(
    benches,
    elsm_bulk_load,
    elsm_monotonic_crud,
    elsm_random_crud,
    elsm_empty_opens
);
criterion_main!(benches);
