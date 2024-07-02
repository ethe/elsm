mod compactor;
mod consistent_hash;
pub(crate) mod index_batch;
pub(crate) mod mem_table;
pub mod oracle;
pub mod record;
pub mod schema;
pub(crate) mod scope;
pub mod serdes;
pub mod stream;
pub mod transaction;
pub(crate) mod utils;
mod version;
pub mod wal;

use std::{
    collections::{BTreeMap, VecDeque},
    error,
    fmt::Debug,
    fs,
    future::Future,
    io, mem,
    path::PathBuf,
    pin::pin,
    sync::Arc,
};

use async_lock::{Mutex, RwLock};
use futures::{
    channel::{
        mpsc::{channel, Sender},
        oneshot,
    },
    StreamExt,
};
use mem_table::MemTable;
use oracle::Oracle;
use record::{Record, RecordType};
use serdes::Encode;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    spawn,
};
use tracing::error;
use transaction::Transaction;
use wal::{provider::WalProvider, WalFile, WalManager, WalWrite, WriteError};

use crate::{
    compactor::Compactor,
    index_batch::IndexBatch,
    oracle::TimeStamp,
    schema::Builder,
    serdes::Decode,
    stream::{buf_stream::BufStream, merge_stream::MergeStream, EStreamImpl, StreamError},
    version::{cleaner::Cleaner, set::VersionSet, Version},
    wal::{FileId, WalRecover},
};

pub type Offset = i64;
pub(crate) type Immutable<S> = Arc<RwLock<VecDeque<(IndexBatch<S>, FileId)>>>;

#[derive(Debug)]
pub enum CompactTask {
    Flush(Option<oneshot::Sender<()>>),
}

#[derive(Debug)]
pub struct DbOption {
    pub path: PathBuf,
    pub max_mem_table_size: usize,
    pub immutable_chunk_num: usize,
    pub major_threshold_with_sst_size: usize,
    pub level_sst_magnification: usize,
    pub max_sst_file_size: usize,
    pub clean_channel_buffer: usize,
}

#[derive(Debug)]
struct MutableShard<S, WP>
where
    S: schema::Schema,
    WP: WalProvider,
{
    mutable: MemTable<S>,
    wal: WalFile<WP::File, S::PrimaryKey, S>,
}

pub struct Db<S, O, WP>
where
    S: schema::Schema,
    O: Oracle<S::PrimaryKey>,
    WP: WalProvider,
{
    option: Arc<DbOption>,
    pub(crate) oracle: O,
    wal_manager: Arc<WalManager<WP>>,
    pub(crate) mutable_shards: RwLock<MutableShard<S, WP>>,
    pub(crate) immutable: Immutable<S>,
    pub(crate) compaction_tx: Mutex<Sender<CompactTask>>,
    pub(crate) version_set: VersionSet<S, WP>,
}

impl<S, O, WP> Db<S, O, WP>
where
    S: schema::Schema,
    O: Oracle<S::PrimaryKey> + 'static,
    WP: WalProvider,
    WP::File: AsyncWrite + AsyncRead,
    io::Error: From<<S as Decode>::Error>,
{
    pub async fn new(
        oracle: O,
        wal_provider: WP,
        option: DbOption,
    ) -> Result<Self, WriteError<<Record<S::PrimaryKey, S> as Encode>::Error>> {
        fs::create_dir_all(&option.path).unwrap();

        let wal_manager = Arc::new(WalManager::new(wal_provider));
        let mutable_shards = RwLock::new(MutableShard {
            mutable: MemTable::default(),
            wal: wal_manager.create_wal_file().await.unwrap(),
        });

        let immutable = Arc::new(RwLock::new(VecDeque::new()));
        let option = Arc::new(option);

        let (task_tx, mut task_rx) = channel(1);
        let (mut cleaner, clean_sender) = Cleaner::new(option.clone());

        let version_set =
            VersionSet::<S, WP>::new(&option, clean_sender.clone(), wal_manager.clone())
                .await
                .map_err(|err| WriteError::Internal(Box::new(err)))?;
        let mut compactor =
            Compactor::<S, WP>::new(immutable.clone(), option.clone(), version_set.clone());

        spawn(async move {
            if let Err(err) = cleaner.listen().await {
                error!("[Cleaner Error]: {}", err)
            }
        });
        spawn(async move {
            loop {
                match task_rx.next().await {
                    None => break,
                    Some(task) => match task {
                        CompactTask::Flush(option_tx) => {
                            if let Err(err) = compactor.check_then_compaction(option_tx).await {
                                error!("[Compaction Error]: {}", err)
                            }
                        }
                    },
                }
            }
        });

        let mut db = Db {
            option,
            oracle,
            wal_manager: wal_manager.clone(),
            mutable_shards,
            immutable,
            compaction_tx: Mutex::new(task_tx),
            version_set,
        };
        let mut file_stream = pin!(wal_manager.wal_provider.list().map_err(WriteError::Io)?);

        while let Some(file) = file_stream.next().await {
            let (file, file_id) = file.map_err(|err| WriteError::Internal(Box::new(err)))?;
            let mut file = wal_manager
                .pack_wal_file(file, file_id)
                .await
                .map_err(WriteError::Io)?;
            db.recover(&mut file)
                .await
                .map_err(|err| WriteError::Internal(Box::new(err)))?;
        }

        Ok(db)
    }
}

impl<S, O, WP> Db<S, O, WP>
where
    S: schema::Schema,
    O: Oracle<S::PrimaryKey>,
    WP: WalProvider,
    WP::File: AsyncWrite,
    io::Error: From<<S as Decode>::Error>,
{
    pub fn new_txn(self: &Arc<Self>) -> Transaction<S, Self> {
        Transaction::new(self.clone())
    }

    pub async fn write(
        &self,
        record_type: RecordType,
        ts: TimeStamp,
        value: S,
    ) -> Result<(), WriteError<<Record<S::PrimaryKey, S> as Encode>::Error>> {
        self.append(record_type, value.primary_key(), ts, Some(value), false)
            .await
    }

    pub async fn remove(
        &self,
        record_type: RecordType,
        ts: TimeStamp,
        key: S::PrimaryKey,
    ) -> Result<(), WriteError<<Record<S::PrimaryKey, S> as Encode>::Error>> {
        self.append(record_type, key, ts, None, false).await
    }

    // TODO(Kould): use hard coding or runtime judgment(`is_recover`)?
    async fn append(
        &self,
        record_type: RecordType,
        key: S::PrimaryKey,
        ts: TimeStamp,
        value: Option<S>,
        is_recover: bool,
    ) -> Result<(), WriteError<<Record<S::PrimaryKey, S> as Encode>::Error>> {
        let wal_manager = self.wal_manager.clone();
        let max_mem_table_size = self.option.max_mem_table_size;

        let freeze = {
            let mut guard = self.mutable_shards.write().await;

            if !is_recover {
                guard
                    .wal
                    .write(Record::new(record_type, &key, ts, value.as_ref()))
                    .await?;
            }
            guard.mutable.insert(key, ts, value);

            if guard.mutable.is_excess(max_mem_table_size) {
                let file_id = guard.wal.file_id();
                if !is_recover {
                    let mut wal_file = wal_manager
                        .create_wal_file()
                        .await
                        .map_err(WriteError::Io)?;
                    mem::swap(&mut guard.wal, &mut wal_file);
                }
                let mut mem_table = MemTable::default();
                mem::swap(&mut guard.mutable, &mut mem_table);

                Some((mem_table, file_id))
            } else {
                None
            }
        };

        if let Some((mem_table, file_id)) = freeze {
            if mem_table.is_empty() {
                return Ok(());
            }
            let mut guard = self.immutable.write().await;

            guard.push_back((Self::freeze(mem_table).await?, file_id));
            if guard.len() > self.option.immutable_chunk_num {
                if let Some(mut guard) = self.compaction_tx.try_lock() {
                    let _ = guard.try_send(CompactTask::Flush(None));
                }
            }
        }
        Ok(())
    }

    pub async fn get(&self, key: &S::PrimaryKey, ts: &TimeStamp) -> Option<S> {
        // Safety: read-only would not break data.
        let (key, ts) = unsafe {
            (
                mem::transmute::<_, &S::PrimaryKey>(key),
                mem::transmute::<_, &TimeStamp>(ts),
            )
        };

        if let Some(value) = self
            .mutable_shards
            .read()
            .await
            .mutable
            .get(key, ts)
            .map(|s| s.cloned())
        {
            return value;
        }
        let guard = self.immutable.read().await;
        for (index_batch, _) in guard.iter().rev() {
            if let Some(value) = index_batch.find(key, ts).await {
                return value;
            }
        }
        drop(guard);

        let guard = self.version_set.current().await;
        if let Ok(Some(record_batch)) = guard.query(key, &self.option).await {
            return S::from_batch(&record_batch, 0).1;
        }
        drop(guard);

        None
    }

    pub async fn range(
        &self,
        lower: Option<&S::PrimaryKey>,
        upper: Option<&S::PrimaryKey>,
        ts: &TimeStamp,
    ) -> Result<MergeStream<S>, StreamError<S::PrimaryKey, S>> {
        let iters = self.inner_range(lower, upper, ts).await?;

        MergeStream::new(iters).await
    }

    pub(crate) async fn inner_range<'s>(
        &'s self,
        lower: Option<&S::PrimaryKey>,
        upper: Option<&S::PrimaryKey>,
        ts: &TimeStamp,
    ) -> Result<Vec<EStreamImpl<S>>, StreamError<S::PrimaryKey, S>> {
        let guard = self.mutable_shards.read().await;
        let mut mem_table_stream = guard.mutable.range(lower, upper, ts).await?;
        let mut items = vec![];

        while let Some(item) = mem_table_stream.next().await {
            let (k, v) = item?;

            items.push((k.clone(), v));
        }
        let mut iters = vec![EStreamImpl::Buf(BufStream::new(items))];
        drop(guard);
        let guard = self.immutable.read().await;

        for (batch, _) in guard.iter() {
            let mut items = Vec::new();
            let mut stream = pin!(batch.range(lower, upper, ts).await?);

            while let Some(item) = stream.next().await {
                let (k, v) = item?;

                items.push((k.clone(), v));
            }
            iters.push(EStreamImpl::Buf(BufStream::new(items)));
        }
        drop(guard);

        self.version_set
            .current()
            .await
            .iters(&mut iters, &self.option, lower, upper)
            .await?;

        Ok(iters)
    }

    pub async fn write_batch(
        &self,
        mut kvs: impl ExactSizeIterator<Item = (S::PrimaryKey, TimeStamp, Option<S>)>,
    ) -> Result<(), WriteError<<Record<S::PrimaryKey, S> as Encode>::Error>> {
        match kvs.len() {
            0 => Ok(()),
            1 => {
                let (key, ts, value) = kvs.next().unwrap();
                self.append(RecordType::Full, key, ts, value, false).await
            }
            len => {
                let (key, ts, value) = kvs.next().unwrap();
                self.append(RecordType::First, key, ts, value, false)
                    .await?;

                for (key, ts, value) in (&mut kvs).take(len - 2) {
                    self.append(RecordType::Middle, key, ts, value, false)
                        .await?;
                }

                let (key, ts, value) = kvs.next().unwrap();
                self.append(RecordType::Last, key, ts, value, false).await
            }
        }
    }

    async fn freeze(
        mem_table: MemTable<S>,
    ) -> Result<IndexBatch<S>, WriteError<<Record<S::PrimaryKey, S> as Encode>::Error>> {
        let mut index = BTreeMap::new();

        let mut builder = S::builder();

        for (offset, (key, value)) in mem_table.data.into_iter().enumerate() {
            builder.add(&key.key, value);
            index.insert(key, offset as u32);
        }
        let batch = builder.finish();

        Ok(IndexBatch { batch, index })
    }

    async fn recover<W>(
        &mut self,
        wal: &mut W,
    ) -> Result<(), WriteError<<Record<S::PrimaryKey, S> as Encode>::Error>>
    where
        W: WalRecover<S::PrimaryKey, S>,
    {
        let mut stream = pin!(wal.recover());
        while let Some(record) = stream.next().await {
            let Record { key, ts, value, .. } =
                record.map_err(|err| WriteError::Internal(Box::new(err)))?;

            self.append(RecordType::Full, key, ts, value, true).await?;
        }
        Ok(())
    }
}

impl<S, O, WP> Oracle<S::PrimaryKey> for Db<S, O, WP>
where
    S: schema::Schema,
    O: Oracle<S::PrimaryKey>,
    WP: WalProvider,
{
    fn start_read(&self) -> TimeStamp {
        self.oracle.start_read()
    }

    fn read_commit(&self, ts: TimeStamp) {
        self.oracle.read_commit(ts)
    }

    fn start_write(&self) -> TimeStamp {
        self.oracle.start_write()
    }

    fn write_commit(
        &self,
        read_at: TimeStamp,
        write_at: TimeStamp,
        in_write: std::collections::HashSet<S::PrimaryKey>,
    ) -> Result<(), oracle::WriteConflict<S::PrimaryKey>> {
        self.oracle.write_commit(read_at, write_at, in_write)
    }
}

pub(crate) trait GetWrite<S>: Oracle<S::PrimaryKey>
where
    S: schema::Schema,
{
    fn get(&self, key: &S::PrimaryKey, ts: &TimeStamp) -> impl Future<Output = Option<S>>
    where
        TimeStamp: Sync;

    fn write(
        &self,
        record_type: RecordType,
        ts: TimeStamp,
        value: S,
    ) -> impl Future<Output = Result<(), Box<dyn error::Error + Send + Sync + 'static>>>;

    async fn remove(
        &self,
        record_type: RecordType,
        ts: TimeStamp,
        key: S::PrimaryKey,
    ) -> Result<(), Box<dyn error::Error + Send + Sync + 'static>>;

    fn write_batch(
        &self,
        kvs: impl ExactSizeIterator<Item = (S::PrimaryKey, TimeStamp, Option<S>)>,
    ) -> impl Future<Output = Result<(), Box<dyn error::Error + Send + Sync + 'static>>>;

    fn inner_range<'a>(
        &'a self,
        lower: Option<&S::PrimaryKey>,
        upper: Option<&S::PrimaryKey>,
        ts: &TimeStamp,
    ) -> impl Future<Output = Result<Vec<EStreamImpl<'a, S>>, StreamError<S::PrimaryKey, S>>>
    where
        S::PrimaryKey: 'a,
        TimeStamp: 'a,
        S: 'a;
}

impl<S, O, WP> GetWrite<S> for Db<S, O, WP>
where
    S: schema::Schema,
    O: Oracle<S::PrimaryKey>,
    WP: WalProvider,
    WP::File: AsyncWrite,
    io::Error: From<<S as Decode>::Error>,
{
    async fn write(
        &self,
        record_type: RecordType,
        ts: TimeStamp,
        value: S,
    ) -> Result<(), Box<dyn error::Error + Send + Sync + 'static>> {
        Db::write(self, record_type, ts, value).await?;
        Ok(())
    }

    async fn remove(
        &self,
        record_type: RecordType,
        ts: TimeStamp,
        key: S::PrimaryKey,
    ) -> Result<(), Box<dyn error::Error + Send + Sync + 'static>> {
        Db::remove(self, record_type, ts, key).await?;
        Ok(())
    }

    async fn get(&self, key: &S::PrimaryKey, ts: &TimeStamp) -> Option<S> {
        Db::get(self, key, ts).await
    }

    async fn write_batch(
        &self,
        kvs: impl ExactSizeIterator<Item = (S::PrimaryKey, TimeStamp, Option<S>)>,
    ) -> Result<(), Box<dyn error::Error + Send + Sync + 'static>> {
        Db::write_batch(self, kvs).await?;
        Ok(())
    }

    async fn inner_range<'a>(
        &'a self,
        lower: Option<&S::PrimaryKey>,
        upper: Option<&S::PrimaryKey>,
        ts: &TimeStamp,
    ) -> Result<Vec<EStreamImpl<'a, S>>, StreamError<S::PrimaryKey, S>>
    where
        S::PrimaryKey: 'a,
        TimeStamp: 'a,
        S: 'a,
    {
        Db::inner_range(self, lower, upper, ts).await
    }
}

impl DbOption {
    pub fn new(path: impl Into<PathBuf> + Send) -> Self {
        DbOption {
            path: path.into(),
            max_mem_table_size: 8 * 1024 * 1024,
            immutable_chunk_num: 5,
            major_threshold_with_sst_size: 10,
            level_sst_magnification: 10,
            max_sst_file_size: 64 * 1024 * 1024,
            clean_channel_buffer: 10,
        }
    }

    pub(crate) fn table_path(&self, gen: &FileId) -> PathBuf {
        self.path.join(format!("{}.parquet", gen))
    }
    pub(crate) fn version_path(&self) -> PathBuf {
        self.path.join("version.log")
    }

    pub(crate) fn is_threshold_exceeded_major<S>(&self, version: &Version<S>, level: usize) -> bool
    where
        S: schema::Schema,
    {
        version.tables_len(level)
            >= (self.major_threshold_with_sst_size * self.level_sst_magnification.pow(level as u32))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{
            Array, BooleanArray, BooleanBuilder, Int16Array, Int16Builder, Int32Array,
            Int32Builder, Int64Array, Int64Builder, Int8Array, Int8Builder, StringArray,
            StringBuilder, StructArray, StructBuilder, UInt16Array, UInt16Builder, UInt32Array,
            UInt32Builder, UInt64Array, UInt64Builder, UInt8Array, UInt8Builder,
        },
        datatypes::{DataType, Field, Fields, SchemaRef},
        record_batch::RecordBatch,
    };
    use elsm_marco::elsm_schema;
    use futures::StreamExt;
    use lazy_static::lazy_static;
    use tempfile::TempDir;
    use tokio::io::{AsyncRead, AsyncWrite};

    use crate::{
        io,
        oracle::LocalOracle,
        record::RecordType,
        schema::Schema,
        stream::merge_stream::MergeStream,
        transaction::CommitError,
        wal::provider::{fs::Fs, in_mem::InMemProvider},
        Builder, Db, DbOption, Decode, Encode,
    };

    #[derive(Debug, Eq, PartialEq)]
    #[elsm_schema]
    pub(crate) struct User {
        #[primary_key]
        pub(crate) id: u64,
        pub(crate) name: String,
        pub(crate) is_human: bool,
        pub(crate) i_number_0: i8,
        pub(crate) i_number_1: i16,
        pub(crate) i_number_2: i32,
        pub(crate) i_number_3: i64,
        pub(crate) u_number_0: u8,
        pub(crate) u_number_1: u16,
        pub(crate) u_number_2: u32,
        pub(crate) u_number_3: u64,
    }

    #[tokio::test]
    async fn test_user() {
        let temp_dir = TempDir::new().unwrap();

        let db = Arc::new(
            Db::new(
                LocalOracle::default(),
                InMemProvider::default(),
                DbOption::new(temp_dir.path().to_path_buf()),
            )
            .await
            .unwrap(),
        );
        let user_0 = UserInner::new(0, "lizeren".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0);
        let user_1 = UserInner::new(1, "2333".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0);
        let user_2 = UserInner::new(2, "ghost".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0);

        let mut t0 = db.new_txn();

        t0.set(user_0.primary_key(), user_0.clone());
        t0.set(user_1.primary_key(), user_1.clone());
        t0.set(user_2.primary_key(), user_2.clone());

        t0.commit().await.unwrap();

        let txn = db.new_txn();

        assert_eq!(txn.get(&user_0.primary_key()).await, Some(user_0));
        assert_eq!(txn.get(&user_1.primary_key()).await, Some(user_1));
        assert_eq!(txn.get(&user_2.primary_key()).await, Some(user_2));
    }

    #[tokio::test]
    async fn read_committed() {
        let temp_dir = TempDir::new().unwrap();

        let db = Arc::new(
            Db::new(
                LocalOracle::default(),
                InMemProvider::default(),
                DbOption::new(temp_dir.path().to_path_buf()),
            )
            .await
            .unwrap(),
        );

        let mut txn = db.new_txn();
        txn.set(
            0,
            UserInner::new(0, "0".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
        );
        txn.set(
            1,
            UserInner::new(1, "1".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
        );
        txn.commit().await.unwrap();

        let mut t0 = db.new_txn();
        let mut t1 = db.new_txn();

        t0.set(0, t0.get(&1).await.unwrap());
        t1.set(1, t1.get(&0).await.unwrap());

        t0.commit().await.unwrap();
        t1.commit().await.unwrap();

        let txn = db.new_txn();

        assert_eq!(
            txn.get(&Arc::from(0)).await,
            Some(UserInner::new(
                1,
                "1".to_string(),
                false,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
            ))
        );
        assert_eq!(
            txn.get(&Arc::from(1)).await,
            Some(UserInner::new(
                0,
                "0".to_string(),
                false,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
            ))
        );
    }

    #[tokio::test]
    async fn range() {
        let temp_dir = TempDir::new().unwrap();

        let db = Arc::new(
            Db::new(
                LocalOracle::default(),
                InMemProvider::default(),
                DbOption::new(temp_dir.path().to_path_buf()),
            )
            .await
            .unwrap(),
        );

        let mut txn = db.new_txn();
        txn.set(
            0,
            UserInner::new(0, "0".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
        );
        txn.set(
            1,
            UserInner::new(1, "1".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
        );
        txn.set(
            2,
            UserInner::new(2, "2".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
        );
        txn.set(
            3,
            UserInner::new(3, "3".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
        );
        txn.commit().await.unwrap();

        let mut iter: MergeStream<UserInner> = db.range(Some(&1), Some(&2), &1).await.unwrap();

        assert_eq!(
            iter.next().await.unwrap().unwrap(),
            (
                1,
                Some(UserInner::new(
                    1,
                    "1".to_string(),
                    false,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0
                ))
            )
        );
        assert_eq!(
            iter.next().await.unwrap().unwrap(),
            (
                2,
                Some(UserInner::new(
                    2,
                    "2".to_string(),
                    false,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0
                ))
            )
        );

        let mut txn_1 = db.new_txn();
        txn_1.set(
            5,
            UserInner::new(5, "5".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
        );
        txn_1.set(
            4,
            UserInner::new(4, "4".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
        );

        let mut txn_2 = db.new_txn();
        txn_2.set(
            5,
            UserInner::new(4, "4".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
        );
        txn_2.set(
            4,
            UserInner::new(5, "5".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
        );
        txn_2.commit().await.unwrap();

        let mut iter = txn_1.range(Some(&1), Some(&4)).await.unwrap();

        assert_eq!(
            iter.next().await.unwrap().unwrap(),
            (
                1,
                Some(UserInner::new(
                    1,
                    "1".to_string(),
                    false,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0
                ))
            )
        );
        assert_eq!(
            iter.next().await.unwrap().unwrap(),
            (
                2,
                Some(UserInner::new(
                    2,
                    "2".to_string(),
                    false,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0
                ))
            )
        );
        assert_eq!(
            iter.next().await.unwrap().unwrap(),
            (
                3,
                Some(UserInner::new(
                    3,
                    "3".to_string(),
                    false,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0
                ))
            )
        );
        assert_eq!(
            iter.next().await.unwrap().unwrap(),
            (
                4,
                Some(UserInner::new(
                    4,
                    "4".to_string(),
                    false,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0
                ))
            )
        );
    }

    #[tokio::test]
    async fn write_conflicts() {
        let temp_dir = TempDir::new().unwrap();

        let db = Arc::new(
            Db::new(
                LocalOracle::default(),
                InMemProvider::default(),
                DbOption::new(temp_dir.path().to_path_buf()),
            )
            .await
            .unwrap(),
        );

        let mut txn = db.new_txn();
        txn.set(
            0,
            UserInner::new(0, "0".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
        );
        txn.set(
            1,
            UserInner::new(1, "1".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
        );
        txn.commit().await.unwrap();

        let mut t0 = db.new_txn();
        let mut t1 = db.new_txn();
        let mut t2 = db.new_txn();

        t0.set(0, t0.get(&1).await.unwrap());
        t1.set(0, t1.get(&0).await.unwrap());
        t1.set(
            2,
            UserInner::new(2, "2".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
        );
        t2.set(
            2,
            UserInner::new(3, "3".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
        );

        t0.commit().await.unwrap();

        let commit = t1.commit().await;
        assert!(commit.is_err());
        assert!(t2.commit().await.is_ok());
        if let Err(CommitError::WriteConflict(keys)) = commit {
            assert_eq!(
                db.new_txn().get(&keys[0]).await,
                Some(UserInner::new(
                    1,
                    "1".to_string(),
                    false,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0
                ))
            );
            return;
        }
        panic!("unreachable");
    }

    fn test_items() -> Vec<UserInner> {
        vec![
            UserInner::new(1, "1".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(2, "2".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(3, "3".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(4, "4".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(5, "5".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(6, "6".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(7, "7".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(8, "8".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(9, "9".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(10, "10".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(20, "20".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(30, "30".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(40, "40".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(50, "50".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(60, "60".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(70, "70".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(80, "80".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(90, "90".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(100, "100".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(200, "200".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(300, "300".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(400, "400".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(500, "500".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(600, "600".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(700, "700".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(800, "800".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(900, "900".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(1000, "1000".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(2000, "2000".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(3000, "3000".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(4000, "4000".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(5000, "5000".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(6000, "6000".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(7000, "7000".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(8000, "8000".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
            UserInner::new(9000, "9000".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
        ]
    }

    #[tokio::test]
    async fn read_from_disk() {
        let temp_dir = TempDir::new().unwrap();

        let db = Arc::new(
            Db::new(
                LocalOracle::default(),
                Fs::new(temp_dir.path()).unwrap(),
                DbOption {
                    // TIPS: kv size in test case is 17
                    path: temp_dir.path().to_path_buf(),
                    max_mem_table_size: 25,
                    immutable_chunk_num: 1,
                    major_threshold_with_sst_size: 5,
                    level_sst_magnification: 10,
                    max_sst_file_size: 2 * 1024 * 1024,
                    clean_channel_buffer: 10,
                },
            )
            .await
            .unwrap(),
        );

        let fn_write = |db: Arc<Db<_, _, _>>, user: UserInner| async move {
            db.write(RecordType::Full, 0, user).await.unwrap();
        };
        for item in test_items() {
            fn_write(db.clone(), item).await;
        }

        assert_eq!(
            db.get(&20, &0).await,
            Some(UserInner::new(
                20,
                "20".to_string(),
                false,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
            ))
        );

        let mut stream: MergeStream<UserInner> = db.range(None, None, &0).await.unwrap();

        let mut results = vec![];
        while let Some(result) = stream.next().await {
            results.push(result.unwrap().1.unwrap());
        }
        assert_eq!(results.len(), test_items().len());
        assert_eq!(results, test_items());

        drop(stream);
        drop(db);

        let db = Db::new(
            LocalOracle::default(),
            Fs::new(temp_dir.path()).unwrap(),
            DbOption {
                // TIPS: kv size in test case is 17
                path: temp_dir.path().to_path_buf(),
                max_mem_table_size: 25,
                immutable_chunk_num: 1,
                major_threshold_with_sst_size: 5,
                level_sst_magnification: 10,
                max_sst_file_size: 2 * 1024 * 1024,
                clean_channel_buffer: 10,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            db.get(&20, &0).await,
            Some(UserInner::new(
                20,
                "20".to_string(),
                false,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
            ))
        );
        let mut stream: MergeStream<UserInner> = db.range(None, None, &0).await.unwrap();

        let mut results = vec![];
        while let Some(result) = stream.next().await {
            results.push(result.unwrap().1.unwrap());
        }
        assert_eq!(results.len(), test_items().len());
        assert_eq!(results, test_items());

        drop(stream);
    }

    #[tokio::test]
    async fn recover_from_wal() {
        let temp_dir = TempDir::new().unwrap();

        let db = Arc::new(
            Db::new(
                LocalOracle::default(),
                Fs::new(temp_dir.path()).unwrap(),
                DbOption::new(temp_dir.path().to_path_buf()),
            )
            .await
            .unwrap(),
        );

        let mut txn = db.new_txn();
        txn.set(
            0,
            UserInner::new(0, "0".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
        );
        txn.set(
            1,
            UserInner::new(1, "1".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0),
        );
        txn.commit().await.unwrap();

        drop(db);

        let db = Db::new(
            LocalOracle::default(),
            Fs::new(temp_dir.path()).unwrap(),
            DbOption::new(temp_dir.path().to_path_buf()),
        )
        .await
        .unwrap();

        assert_eq!(
            db.get(&0, &1).await,
            Some(UserInner::new(
                0,
                "0".to_string(),
                false,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
            )),
        );
        assert_eq!(
            db.get(&1, &1).await,
            Some(UserInner::new(
                1,
                "1".to_string(),
                false,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
            )),
        );
    }
}
