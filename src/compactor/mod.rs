use std::{cmp, collections::VecDeque, fmt::Debug, mem, pin::pin, sync::Arc};

use futures::{channel::oneshot, StreamExt};
use parquet::arrow::AsyncArrowWriter;
use thiserror::Error;
use tokio::fs::File;
use ulid::Ulid;

use crate::{
    index_batch::IndexBatch,
    schema::{Builder, Schema},
    scope::Scope,
    serdes::Encode,
    stream::{
        level_stream::LevelStream, merge_stream::MergeStream, table_stream::TableStream,
        EStreamImpl, StreamError,
    },
    version::{edit::VersionEdit, set::VersionSet, Version, VersionError, MAX_LEVEL},
    wal::{provider::WalProvider, FileId},
    DbOption, Immutable,
};

pub(crate) struct Compactor<S, WP>
where
    S: Schema,
    WP: WalProvider,
{
    pub(crate) option: Arc<DbOption>,
    pub(crate) immutable: Immutable<S>,
    pub(crate) version_set: VersionSet<S, WP>,
}

impl<S, WP> Compactor<S, WP>
where
    S: Schema,
    WP: WalProvider,
{
    pub(crate) fn new(
        immutable: Immutable<S>,
        option: Arc<DbOption>,
        version_set: VersionSet<S, WP>,
    ) -> Self {
        Compactor::<S, WP> {
            option,
            immutable,
            version_set,
        }
    }

    pub(crate) async fn check_then_compaction(
        &mut self,
        option_tx: Option<oneshot::Sender<()>>,
    ) -> Result<(), CompactionError<S>> {
        let mut guard = self.immutable.write().await;

        if guard.len() > self.option.immutable_chunk_num {
            let excess = guard.split_off(self.option.immutable_chunk_num);

            if let Some(scope) =
                Self::minor_compaction(&self.option, mem::replace(&mut guard, excess)).await?
            {
                let version_ref = self.version_set.current().await;
                let mut version_edits = vec![];
                let mut delete_gens = vec![];

                if self.option.is_threshold_exceeded_major(&version_ref, 0) {
                    Self::major_compaction(
                        &version_ref,
                        &self.option,
                        &scope.min,
                        &scope.max,
                        &mut version_edits,
                        &mut delete_gens,
                    )
                    .await?;
                }
                version_edits.insert(0, VersionEdit::Add { level: 0, scope });

                self.version_set
                    .apply_edits(version_edits, Some(delete_gens), false)
                    .await
                    .map_err(CompactionError::Version)?;
            }
        }
        if let Some(tx) = option_tx {
            let _ = tx.send(());
        }
        Ok(())
    }

    pub(crate) async fn minor_compaction(
        option: &DbOption,
        batches: VecDeque<(IndexBatch<S>, FileId)>,
    ) -> Result<Option<Scope<S::PrimaryKey>>, CompactionError<S>> {
        if !batches.is_empty() {
            let mut min = None;
            let mut max = None;

            let gen = FileId::new();
            let mut wal_ids = Vec::with_capacity(batches.len());

            let mut writer = AsyncArrowWriter::try_new(
                File::create(option.table_path(&gen))
                    .await
                    .map_err(CompactionError::Io)?,
                S::inner_schema(),
                None,
            )
            .map_err(CompactionError::Parquet)?;

            for (batch, wal_id) in batches {
                if let Some((batch_min, batch_max)) = batch.scope() {
                    if matches!(min.as_ref().map(|min| min > batch_min), Some(true) | None) {
                        min = Some(batch_min.clone())
                    }
                    if matches!(max.as_ref().map(|max| max < batch_max), Some(true) | None) {
                        max = Some(batch_max.clone())
                    }
                }
                writer
                    .write(&batch.batch)
                    .await
                    .map_err(CompactionError::Parquet)?;
                wal_ids.push(wal_id);
            }
            writer.close().await.map_err(CompactionError::Parquet)?;
            return Ok(Some(Scope {
                min: min.ok_or(CompactionError::EmptyLevel)?,
                max: max.ok_or(CompactionError::EmptyLevel)?,
                gen,
                wal_ids: Some(wal_ids),
            }));
        }
        Ok(None)
    }

    pub(crate) async fn major_compaction(
        version: &Version<S>,
        option: &DbOption,
        mut min: &S::PrimaryKey,
        mut max: &S::PrimaryKey,
        version_edits: &mut Vec<VersionEdit<S::PrimaryKey>>,
        delete_gens: &mut Vec<FileId>,
    ) -> Result<(), CompactionError<S>> {
        let mut level = 0;

        while level < MAX_LEVEL - 2 {
            if !option.is_threshold_exceeded_major(version, level) {
                break;
            }

            let mut meet_scopes_l = Vec::new();
            {
                let index = Version::<S>::scope_search(min, &version.level_slice[level]);

                for scope in version.level_slice[level][index..].iter() {
                    if scope.is_between(min) || scope.is_between(max) {
                        meet_scopes_l.push(scope);
                    }
                }
                if meet_scopes_l.is_empty() {
                    return Ok(());
                }
            }
            let mut meet_scopes_ll = Vec::new();
            {
                if !version.level_slice[level + 1].is_empty() {
                    let min_key = &meet_scopes_l
                        .first()
                        .ok_or(CompactionError::EmptyLevel)?
                        .min;
                    let max_key = &meet_scopes_l.last().ok_or(CompactionError::EmptyLevel)?.max;
                    min = min_key;
                    max = max_key;

                    let min_index =
                        Version::<S>::scope_search(min_key, &version.level_slice[level + 1]);
                    let max_index =
                        Version::<S>::scope_search(max_key, &version.level_slice[level + 1]);

                    let next_level_len = version.level_slice[level + 1].len();
                    for scope in version.level_slice[level + 1]
                        [min_index..cmp::min(max_index + 1, next_level_len - 1)]
                        .iter()
                    {
                        if scope.is_between(min) || scope.is_between(max) {
                            meet_scopes_ll.push(scope);
                        }
                    }
                }
            }
            let mut streams = Vec::with_capacity(meet_scopes_l.len() + meet_scopes_ll.len());

            // This Level
            if level == 0 {
                for scope in meet_scopes_l.iter() {
                    streams.push(EStreamImpl::Table(
                        TableStream::new(option, &scope.gen, None, None)
                            .await
                            .map_err(CompactionError::Stream)?,
                    ));
                }
            } else {
                let gens = meet_scopes_l
                    .iter()
                    .map(|scope| scope.gen)
                    .collect::<Vec<_>>();
                streams.push(EStreamImpl::Level(
                    LevelStream::new(option, gens, Some(min), Some(max))
                        .await
                        .map_err(CompactionError::Stream)?,
                ));
            }
            // Next Level
            let gens = meet_scopes_ll
                .iter()
                .map(|scope| scope.gen)
                .collect::<Vec<_>>();
            streams.push(EStreamImpl::Level(
                LevelStream::new(option, gens, None, None)
                    .await
                    .map_err(CompactionError::Stream)?,
            ));
            let stream = MergeStream::<S>::new(streams)
                .await
                .map_err(CompactionError::Stream)?;

            let mut stream = pin!(stream);
            let mut builder = S::builder();
            let mut written_size = 0;
            let mut min = None;
            let mut max = None;

            while let Some(result) = stream.next().await {
                let (key, value) = result.map_err(CompactionError::Stream)?;
                if min.is_none() {
                    min = Some(key.clone())
                }
                max = Some(key.clone());

                written_size += key.size();
                builder.add(&key, value);

                if written_size >= option.max_sst_file_size {
                    Self::build_table(
                        option,
                        version_edits,
                        level,
                        &mut builder,
                        &mut min,
                        &mut max,
                    )
                    .await?;
                    written_size = 0;
                }
            }
            if written_size > 0 {
                Self::build_table(
                    option,
                    version_edits,
                    level,
                    &mut builder,
                    &mut min,
                    &mut max,
                )
                .await?;
            }
            for scope in meet_scopes_l {
                version_edits.push(VersionEdit::Remove {
                    level: level as u8,
                    gen: scope.gen,
                });
                delete_gens.push(scope.gen);
            }
            for scope in meet_scopes_ll {
                version_edits.push(VersionEdit::Remove {
                    level: (level + 1) as u8,
                    gen: scope.gen,
                });
                delete_gens.push(scope.gen);
            }
            level += 1;
        }

        Ok(())
    }

    async fn build_table(
        option: &DbOption,
        version_edits: &mut Vec<VersionEdit<S::PrimaryKey>>,
        level: usize,
        builder: &mut S::Builder,
        min: &mut Option<S::PrimaryKey>,
        max: &mut Option<S::PrimaryKey>,
    ) -> Result<(), CompactionError<S>> {
        assert!(min.is_some());
        assert!(max.is_some());

        let gen = Ulid::new();
        let batch = builder.finish();
        let mut writer = AsyncArrowWriter::try_new(
            File::create(option.table_path(&gen))
                .await
                .map_err(CompactionError::Io)?,
            S::inner_schema(),
            None,
        )
        .map_err(CompactionError::Parquet)?;
        writer
            .write(&batch)
            .await
            .map_err(CompactionError::Parquet)?;
        writer.close().await.map_err(CompactionError::Parquet)?;
        version_edits.push(VersionEdit::Add {
            level: (level + 1) as u8,
            scope: Scope {
                min: min.take().ok_or(CompactionError::EmptyLevel)?,
                max: max.take().ok_or(CompactionError::EmptyLevel)?,
                gen,
                wal_ids: None,
            },
        });
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum CompactionError<S>
where
    S: Schema,
{
    #[error("compaction io error: {0}")]
    Io(#[source] std::io::Error),
    #[error("compaction parquet error: {0}")]
    Parquet(#[source] parquet::errors::ParquetError),
    #[error("compaction version error: {0}")]
    Version(#[source] VersionError<S>),
    #[error("compaction stream error: {0}")]
    Stream(#[source] StreamError<S::PrimaryKey, S>),
    #[error("the level being compacted does not have a table")]
    EmptyLevel,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        fs::File,
    };

    use futures::channel::mpsc::channel;
    use parquet::arrow::ArrowWriter;
    use tempfile::TempDir;

    use crate::{
        compactor::Compactor,
        index_batch::IndexBatch,
        mem_table::InternalKey,
        schema,
        schema::Builder,
        scope::Scope,
        tests::UserInner,
        version::{edit::VersionEdit, Version},
        wal::{provider::in_mem::InMemProvider, FileId},
        DbOption,
    };

    async fn build_index_batch<S>(items: Vec<(S, bool)>) -> IndexBatch<S>
    where
        S: schema::Schema,
    {
        let mut index = BTreeMap::new();
        let mut builder = S::builder();

        for (offset, (schema, is_deleted)) in items.into_iter().enumerate() {
            index.insert(
                InternalKey {
                    key: schema.primary_key(),
                    ts: 0,
                },
                offset as u32,
            );
            builder.add(&schema.primary_key(), is_deleted.then(|| schema));
        }

        let batch = builder.finish();

        IndexBatch { batch, index }
    }

    async fn build_parquet_table<S: schema::Schema>(
        option: &DbOption,
        gen: FileId,
        items: Vec<(S, bool)>,
    ) {
        let batch = build_index_batch(items).await;

        let mut writer = ArrowWriter::try_new(
            File::create(option.table_path(&gen)).unwrap(),
            S::inner_schema(),
            None,
        )
        .unwrap();
        writer.write(&batch.batch).unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn minor_compaction() {
        let temp_dir = TempDir::new().unwrap();

        let option = DbOption::new(temp_dir.path().to_path_buf());

        let batch_1 = build_index_batch::<UserInner>(vec![
            (
                UserInner::new(3, "3".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                false,
            ),
            (
                UserInner::new(5, "5".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                false,
            ),
            (
                UserInner::new(6, "6".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                false,
            ),
        ])
        .await;
        let batch_2 = build_index_batch::<UserInner>(vec![
            (
                UserInner::new(4, "4".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                false,
            ),
            (
                UserInner::new(2, "2".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                false,
            ),
            (
                UserInner::new(1, "1".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                false,
            ),
        ])
        .await;

        let scope = Compactor::<UserInner, InMemProvider>::minor_compaction(
            &option,
            VecDeque::from(vec![(batch_2, FileId::new()), (batch_1, FileId::new())]),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(scope.min, 1);
        assert_eq!(scope.max, 6);
    }

    #[tokio::test]
    async fn major_compaction() {
        let temp_dir = TempDir::new().unwrap();

        let mut option = DbOption::new(temp_dir.path().to_path_buf());
        option.major_threshold_with_sst_size = 2;

        // level 1
        let table_gen_1 = FileId::new();
        let table_gen_2 = FileId::new();
        build_parquet_table(
            &option,
            table_gen_1,
            vec![
                (
                    UserInner::new(1, "1".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                    false,
                ),
                (
                    UserInner::new(2, "2".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                    false,
                ),
                (
                    UserInner::new(3, "3".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                    false,
                ),
            ],
        )
        .await;
        build_parquet_table(
            &option,
            table_gen_2,
            vec![
                (
                    UserInner::new(4, "4".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                    false,
                ),
                (
                    UserInner::new(5, "5".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                    false,
                ),
                (
                    UserInner::new(6, "6".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                    false,
                ),
            ],
        )
        .await;

        // level 2
        let table_gen_3 = FileId::new();
        let table_gen_4 = FileId::new();
        let table_gen_5 = FileId::new();
        build_parquet_table(
            &option,
            table_gen_3,
            vec![
                (
                    UserInner::new(1, "1".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                    false,
                ),
                (
                    UserInner::new(2, "2".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                    false,
                ),
                (
                    UserInner::new(3, "3".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                    false,
                ),
            ],
        )
        .await;
        build_parquet_table(
            &option,
            table_gen_4,
            vec![
                (
                    UserInner::new(4, "4".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                    false,
                ),
                (
                    UserInner::new(5, "5".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                    false,
                ),
                (
                    UserInner::new(6, "6".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                    false,
                ),
            ],
        )
        .await;
        build_parquet_table(
            &option,
            table_gen_5,
            vec![
                (
                    UserInner::new(7, "7".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                    false,
                ),
                (
                    UserInner::new(8, "8".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                    false,
                ),
                (
                    UserInner::new(9, "9".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
                    false,
                ),
            ],
        )
        .await;

        let (sender, _) = channel(1);

        let mut version = Version::<UserInner> {
            num: 0,
            level_slice: Version::<UserInner>::level_slice_new(),
            clean_sender: sender,
        };
        version.level_slice[0].push(Scope {
            min: 1,
            max: 3,
            gen: table_gen_1,
            wal_ids: None,
        });
        version.level_slice[0].push(Scope {
            min: 4,
            max: 6,
            gen: table_gen_2,
            wal_ids: None,
        });
        version.level_slice[1].push(Scope {
            min: 1,
            max: 3,
            gen: table_gen_3,
            wal_ids: None,
        });
        version.level_slice[1].push(Scope {
            min: 4,
            max: 6,
            gen: table_gen_4,
            wal_ids: None,
        });
        version.level_slice[1].push(Scope {
            min: 7,
            max: 9,
            gen: table_gen_5,
            wal_ids: None,
        });

        let min = 2;
        let max = 5;
        let mut version_edits = Vec::new();

        Compactor::<UserInner, InMemProvider>::major_compaction(
            &version,
            &option,
            &min,
            &max,
            &mut version_edits,
            &mut vec![],
        )
        .await
        .unwrap();

        if let VersionEdit::Add { level, scope } = &version_edits[0] {
            assert_eq!(*level, 1);
            assert_eq!(scope.min, 1);
            assert_eq!(scope.max, 6);
        }
        assert_eq!(
            version_edits[1..5].to_vec(),
            vec![
                VersionEdit::Remove {
                    level: 0,
                    gen: table_gen_1,
                },
                VersionEdit::Remove {
                    level: 0,
                    gen: table_gen_2,
                },
                VersionEdit::Remove {
                    level: 1,
                    gen: table_gen_3,
                },
                VersionEdit::Remove {
                    level: 1,
                    gen: table_gen_4,
                },
            ]
        )
    }
}
