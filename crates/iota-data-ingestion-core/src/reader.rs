// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap, ffi::OsString, fs, num::NonZeroUsize, path::PathBuf, sync::Arc,
    time::Duration,
};

use backoff::backoff::Backoff;
use futures::StreamExt;
use iota_config::{
    node::ArchiveReaderConfig,
    object_storage_config::{ObjectStoreConfig, ObjectStoreType},
};
use iota_metrics::spawn_monitored_task;
use iota_rest_api::Client;
use iota_storage::blob::Blob;
use iota_types::{
    full_checkpoint_content::CheckpointData, messages_checkpoint::CheckpointSequenceNumber,
};
use notify::{RecursiveMode, Watcher};
use object_store::{ObjectStore, path::Path};
use tap::pipe::Pipe;
use tokio::{
    sync::{
        mpsc::{self, error::TryRecvError},
        oneshot,
    },
    time::timeout,
};
use tracing::{debug, error, info};

use crate::{
    IngestionError, IngestionResult, create_remote_store_client,
    executor::MAX_CHECKPOINTS_IN_PROGRESS,
    history::{
        manifest::{Manifest, read_manifest_from_bytes},
        reader::HistoricalReader,
    },
};

const CHECKPOINT_FILE_SUFFIX: &str = "chk";
const LIVE_DIR_NAME: &str = "live";
const INGESTION_DIR_NAME: &str = "ingestion";

type CheckpointResult = IngestionResult<(Arc<CheckpointData>, usize)>;

/// Implements a checkpoint reader that monitors a local directory.
/// Designed for setups where the indexer daemon is colocated with FN.
/// This implementation is push-based and utilizes the inotify API.
pub struct CheckpointReader {
    /// Filesystem path to the local checkpoint directory.
    path: PathBuf,
    /// URL for the remote checkpoint store.
    remote_store_url: Option<String>,
    /// Additional options for configuring the remote store.
    remote_store_options: Vec<(String, String)>,
    /// Start fetch from the current checkpoint sequence.
    current_checkpoint_number: CheckpointSequenceNumber,
    last_pruned_watermark: CheckpointSequenceNumber,
    /// Channel for sending checkpoints to WorkerPools.
    checkpoint_sender: mpsc::Sender<Arc<CheckpointData>>,
    /// Channel receiver for receiving notifications of processed checkpoints
    /// from [`WorkerPools`](crate::WorkerPool).
    processed_receiver: mpsc::Receiver<CheckpointSequenceNumber>,
    /// Checkpoints fetched from the remote store.
    remote_fetcher_receiver: Option<mpsc::Receiver<CheckpointResult>>,
    /// Signal when the reader should exit.
    exit_receiver: oneshot::Receiver<()>,
    options: ReaderOptions,
    data_limiter: DataLimiter,
    /// Historical checkpoint reader for fetching checkpoints from genesis up to
    /// tip of the network.
    historical_reader: Option<Arc<HistoricalReader>>,
    /// Remote checkpoint reader for fetching checkpoints from the network.
    remote_store: Option<Arc<RemoteStore>>,
}

/// Options for configuring how the checkpoint reader fetches new checkpoints.
#[derive(Clone)]
pub struct ReaderOptions {
    /// How often to check for new checkpoints, lower values mean faster
    /// detection but more CPU usage.
    ///
    /// Default: 100ms.
    pub tick_interval_ms: u64,
    /// Network request timeout, it applies to remote store operations.
    ///
    /// Default: 5 seconds.
    pub timeout_secs: u64,
    /// Number of maximum concurrent requests to the remote store. Increase it
    /// for backfills, higher values increase throughput but use more resources.
    ///
    /// Default: 10.
    pub batch_size: usize,
    /// Maximum memory (bytes) for batch checkpoint processing to prevent OOM
    /// errors. Zero indicates no limit.
    ///
    /// Default: 0.
    pub data_limit: usize,
}

impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            tick_interval_ms: 100,
            timeout_secs: 5,
            batch_size: 10,
            data_limit: 0,
        }
    }
}

enum RemoteStore {
    ObjectStore(Box<dyn ObjectStore>),
    Rest(iota_rest_api::Client),
    Hybrid(Box<dyn ObjectStore>, iota_rest_api::Client),
}

impl CheckpointReader {
    /// Represents a single iteration of the reader.
    /// Reads files in a local directory, validates them, and forwards
    /// `CheckpointData` to the executor.
    async fn read_local_files(&self) -> IngestionResult<Vec<Arc<CheckpointData>>> {
        let mut files = vec![];
        for entry in fs::read_dir(self.path.clone())? {
            let entry = entry?;
            let filename = entry.file_name();
            if let Some(sequence_number) = Self::checkpoint_number_from_file_path(&filename) {
                if sequence_number >= self.current_checkpoint_number {
                    files.push((sequence_number, entry.path()));
                }
            }
        }
        files.sort();
        debug!("unprocessed local files {:?}", files);
        let mut checkpoints = vec![];
        for (_, filename) in files.iter().take(MAX_CHECKPOINTS_IN_PROGRESS) {
            let checkpoint = Blob::from_bytes::<Arc<CheckpointData>>(&fs::read(filename)?)
                .map_err(|err| IngestionError::DeserializeCheckpoint(err.to_string()))?;
            if self.exceeds_capacity(checkpoint.checkpoint_summary.sequence_number) {
                break;
            }
            checkpoints.push(checkpoint);
        }
        Ok(checkpoints)
    }

    fn exceeds_capacity(&self, checkpoint_number: CheckpointSequenceNumber) -> bool {
        ((MAX_CHECKPOINTS_IN_PROGRESS as u64 + self.last_pruned_watermark) <= checkpoint_number)
            || self.data_limiter.exceeds()
    }

    /// Constructs a file path for a checkpoint file based on the checkpoint
    /// sequence number for Live object store.
    fn live_object_store_file_path(chk_seq_num: CheckpointSequenceNumber) -> Path {
        Path::from(INGESTION_DIR_NAME)
            .child(LIVE_DIR_NAME)
            .child(format!("{chk_seq_num}.{CHECKPOINT_FILE_SUFFIX}"))
    }

    /// Constructs a file path for a checkpoint file based on the checkpoint
    /// sequence number for a generic object store.
    fn object_store_file_path(chk_seq_num: CheckpointSequenceNumber) -> Path {
        Path::from(format!("{chk_seq_num}.{CHECKPOINT_FILE_SUFFIX}"))
    }

    /// Helper function to fetch and deserialize a single checkpoint from an
    /// object store.
    async fn fetch_from_object_store_internal(
        store: &dyn ObjectStore,
        path: &Path,
    ) -> CheckpointResult {
        let response = store.get(path).await?;
        let bytes = response.bytes().await?;
        Ok((
            Blob::from_bytes::<Arc<CheckpointData>>(&bytes)
                .map_err(|err| IngestionError::DeserializeCheckpoint(err.to_string()))?,
            bytes.len(),
        ))
    }

    /// Fetches a checkpoint from the given object store, optionally using a
    /// historical reader.
    async fn fetch_from_object_store(
        store: &dyn ObjectStore,
        historical_reader: Option<&HistoricalReader>,
        checkpoint_number: CheckpointSequenceNumber,
    ) -> IngestionResult<(Arc<CheckpointData>, usize)> {
        if let Some(historical_reader) = historical_reader {
            return Self::fetch_from_hybrid_object_store(
                historical_reader,
                store,
                checkpoint_number,
            )
            .await;
        }

        Self::fetch_from_object_store_internal(
            store,
            &Self::object_store_file_path(checkpoint_number),
        )
        .await
    }

    /// Attempts to fetch a checkpoint from the historical object store first,
    /// falling back to the live object store if not found.
    async fn fetch_from_hybrid_object_store(
        historical_reader: &HistoricalReader,
        live: &dyn ObjectStore,
        checkpoint_number: CheckpointSequenceNumber,
    ) -> IngestionResult<(Arc<CheckpointData>, usize)> {
        if let Some((checkpoint, size)) =
            Self::fetch_from_historical_object_store(historical_reader, checkpoint_number).await?
        {
            debug!(
                "checkpoint fetched from historical object store: {}",
                checkpoint.checkpoint_summary.sequence_number
            );
            return Ok((checkpoint, size));
        };

        Self::fetch_from_live_object_store(live, checkpoint_number).await
    }

    async fn fetch_from_live_object_store(
        store: &dyn ObjectStore,
        checkpoint_number: CheckpointSequenceNumber,
    ) -> IngestionResult<(Arc<CheckpointData>, usize)> {
        Self::fetch_from_object_store_internal(
            store,
            &Self::live_object_store_file_path(checkpoint_number),
        )
        .await
    }

    async fn fetch_from_historical_object_store(
        historical_reader: &HistoricalReader,
        checkpoint_number: CheckpointSequenceNumber,
    ) -> IngestionResult<Option<(Arc<CheckpointData>, usize)>> {
        historical_reader.sync_manifest_once().await?;
        let latest_available_checkpoint = historical_reader.latest_available_checkpoint().await?;
        debug!("historical latest_available_checkpoint: {latest_available_checkpoint}");
        if checkpoint_number > latest_available_checkpoint {
            return Ok(None);
        }
        let checkpoint = historical_reader
            .iter_for_range(checkpoint_number..checkpoint_number + 1)
            .await?
            .next();

        checkpoint
            .map(|chk| bcs::serialized_size(&chk).map(|size| (Arc::new(chk), size)))
            .transpose()
            .map_err(Into::into)
    }

    async fn fetch_from_full_node(
        client: &Client,
        checkpoint_number: CheckpointSequenceNumber,
    ) -> IngestionResult<(Arc<CheckpointData>, usize)> {
        let checkpoint = client.get_full_checkpoint(checkpoint_number).await?;
        let size = bcs::serialized_size(&checkpoint)?;
        Ok((Arc::new(checkpoint), size))
    }

    async fn remote_fetch_checkpoint_internal(
        store: &RemoteStore,
        historical_reader: Option<&HistoricalReader>,
        checkpoint_number: CheckpointSequenceNumber,
    ) -> IngestionResult<(Arc<CheckpointData>, usize)> {
        match store {
            RemoteStore::ObjectStore(store) => {
                Self::fetch_from_object_store(store, historical_reader, checkpoint_number).await
            }
            RemoteStore::Rest(client) => {
                Self::fetch_from_full_node(client, checkpoint_number).await
            }
            RemoteStore::Hybrid(store, client) => {
                match Self::fetch_from_full_node(client, checkpoint_number).await {
                    Ok(result) => Ok(result),
                    Err(_) => {
                        Self::fetch_from_object_store(store, historical_reader, checkpoint_number)
                            .await
                    }
                }
            }
        }
    }

    async fn remote_fetch_checkpoint(
        store: &RemoteStore,
        historical_reader: Option<&HistoricalReader>,
        checkpoint_number: CheckpointSequenceNumber,
    ) -> IngestionResult<(Arc<CheckpointData>, usize)> {
        let mut backoff = backoff::ExponentialBackoff::default();
        backoff.max_elapsed_time = Some(Duration::from_secs(60));
        backoff.initial_interval = Duration::from_millis(100);
        backoff.current_interval = backoff.initial_interval;
        backoff.multiplier = 1.0;
        loop {
            match Self::remote_fetch_checkpoint_internal(
                store,
                historical_reader,
                checkpoint_number,
            )
            .await
            {
                Ok(data) => return Ok(data),
                Err(err) => match backoff.next_backoff() {
                    Some(duration) => {
                        if !err.to_string().contains("404") {
                            debug!(
                                "remote reader retry in {} ms. Error is {:?}",
                                duration.as_millis(),
                                err
                            );
                        }
                        tokio::time::sleep(duration).await
                    }
                    None => return Err(err),
                },
            }
        }
    }

    async fn create_remote_store(&self) -> (Option<RemoteStore>, Option<HistoricalReader>) {
        let Some(url) = self.remote_store_url.as_ref() else {
            return (None, None);
        };

        if let Some((fn_url, remote_url)) = url.split_once('|') {
            let object_store = create_remote_store_client(
                remote_url.to_string(),
                self.remote_store_options.clone(),
                self.options.timeout_secs,
            )
            .expect("failed to create remote store client");
            let historical_reader =
                Self::historical_reader(&object_store, url.clone(), self.options.batch_size).await;
            return (
                Some(RemoteStore::Hybrid(
                    object_store,
                    iota_rest_api::Client::new(fn_url),
                )),
                historical_reader,
            );
        }

        if url.ends_with("/api/v1") {
            return (
                Some(RemoteStore::Rest(iota_rest_api::Client::new(url))),
                None,
            );
        }

        let object_store = create_remote_store_client(
            url.clone(),
            self.remote_store_options.clone(),
            self.options.timeout_secs,
        )
        .expect("failed to create remote store client");

        let historical_reader =
            Self::historical_reader(&object_store, url.clone(), self.options.batch_size).await;

        (
            Some(RemoteStore::ObjectStore(object_store)),
            historical_reader,
        )
    }

    fn start_remote_fetcher(
        &mut self,
    ) -> mpsc::Receiver<IngestionResult<(Arc<CheckpointData>, usize)>> {
        let batch_size = self.options.batch_size;
        let start_checkpoint = self.current_checkpoint_number;
        let (sender, receiver) = mpsc::channel(batch_size);

        let Some(remote_store) = self.remote_store.as_ref().map(Arc::clone) else {
            panic!("remote store not initialized")
        };
        let historical_reader = self.historical_reader.as_ref().map(Arc::clone);

        spawn_monitored_task!(async move {
            let mut checkpoint_stream = (start_checkpoint..u64::MAX)
                .map(|checkpoint_number| {
                    Self::remote_fetch_checkpoint(
                        &remote_store,
                        historical_reader.as_deref(),
                        checkpoint_number,
                    )
                })
                .pipe(futures::stream::iter)
                .buffered(batch_size);

            while let Some(checkpoint) = checkpoint_stream.next().await {
                if sender.send(checkpoint).await.is_err() {
                    info!("remote reader dropped");
                    break;
                }
            }
        });
        receiver
    }

    /// Creates a new historical reader.
    ///
    /// It checks if the object store does contain a Manifest file and
    /// validates it. If conditions are met a [`HsitoricalReader`] is
    /// instantiated.
    async fn historical_reader(
        store: &dyn ObjectStore,
        url: String,
        batch_size: usize,
    ) -> Option<HistoricalReader> {
        if let Ok(manifest) = store.get(&Manifest::file_path()).await {
            let is_manifest_valid = manifest
                .bytes()
                .await
                .map_err(Into::into)
                .and_then(|bytes| read_manifest_from_bytes(bytes.to_vec()))
                .is_ok();

            return is_manifest_valid
                .then_some({
                    let config = ArchiveReaderConfig {
                        download_concurrency: NonZeroUsize::new(batch_size)
                            .expect("batch size must be greater than zero"),
                        remote_store_config: ObjectStoreConfig {
                            object_store: Some(ObjectStoreType::S3),
                            object_store_connection_limit: 20,
                            aws_endpoint: Some(url),
                            aws_virtual_hosted_style_request: true,
                            aws_region: Some("weur".to_string()),
                            no_sign_request: true,
                            ..Default::default()
                        },
                        use_for_pruning_watermark: false,
                    };
                    HistoricalReader::new(config)
                        .inspect_err(|e| {
                            tracing::error!("unable to instantiate historical reader: {e}")
                        })
                        .ok()
                })
                .flatten();
        }
        None
    }

    fn remote_fetch(&mut self) -> Vec<Arc<CheckpointData>> {
        let mut checkpoints = vec![];
        if self.remote_fetcher_receiver.is_none() {
            self.remote_fetcher_receiver = Some(self.start_remote_fetcher());
        }
        while !self.exceeds_capacity(self.current_checkpoint_number + checkpoints.len() as u64) {
            match self.remote_fetcher_receiver.as_mut().unwrap().try_recv() {
                Ok(Ok((checkpoint, size))) => {
                    self.data_limiter.add(&checkpoint, size);
                    checkpoints.push(checkpoint);
                }
                Ok(Err(err)) => {
                    error!("remote reader transient error {:?}", err);
                    self.remote_fetcher_receiver = None;
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    error!("remote reader channel disconnect error");
                    self.remote_fetcher_receiver = None;
                    break;
                }
                Err(TryRecvError::Empty) => break,
            }
        }
        checkpoints
    }

    async fn sync(&mut self) -> IngestionResult<()> {
        let backoff = backoff::ExponentialBackoff::default();
        let mut checkpoints = backoff::future::retry(backoff, || async {
            self.read_local_files().await.map_err(|err| {
                info!("transient local read error {:?}", err);
                backoff::Error::transient(err)
            })
        })
        .await?;

        let mut read_source: &str = "local";
        if self.remote_store.is_some()
            && (checkpoints.is_empty()
                || checkpoints[0].checkpoint_summary.sequence_number
                    > self.current_checkpoint_number)
        {
            checkpoints = self.remote_fetch();
            read_source = "remote";
        } else {
            // cancel remote fetcher execution because local reader has made progress
            self.remote_fetcher_receiver = None;
        }

        info!(
            "Read from {}. Current checkpoint number: {}, pruning watermark: {}, new updates: {:?}",
            read_source,
            self.current_checkpoint_number,
            self.last_pruned_watermark,
            checkpoints.len(),
        );
        for checkpoint in checkpoints {
            if read_source == "local"
                && checkpoint.checkpoint_summary.sequence_number > self.current_checkpoint_number
            {
                break;
            }
            assert_eq!(
                checkpoint.checkpoint_summary.sequence_number,
                self.current_checkpoint_number
            );
            self.checkpoint_sender.send(checkpoint).await.map_err(|_| {
                IngestionError::Channel(
                    "unable to send checkpoint to executor, receiver half closed".to_owned(),
                )
            })?;
            self.current_checkpoint_number += 1;
        }
        Ok(())
    }

    /// Cleans the local directory by removing all processed checkpoint files.
    fn gc_processed_files(&mut self, watermark: CheckpointSequenceNumber) -> IngestionResult<()> {
        info!("cleaning processed files, watermark is {}", watermark);
        self.data_limiter.gc(watermark);
        self.last_pruned_watermark = watermark;
        for entry in fs::read_dir(self.path.clone())? {
            let entry = entry?;
            let filename = entry.file_name();
            if let Some(sequence_number) = Self::checkpoint_number_from_file_path(&filename) {
                if sequence_number < watermark {
                    fs::remove_file(entry.path())?;
                }
            }
        }
        Ok(())
    }

    fn checkpoint_number_from_file_path(file_name: &OsString) -> Option<CheckpointSequenceNumber> {
        file_name
            .to_str()
            .and_then(|s| s.rfind('.').map(|pos| &s[..pos]))
            .and_then(|s| s.parse().ok())
    }

    pub fn initialize(
        path: PathBuf,
        starting_checkpoint_number: CheckpointSequenceNumber,
        remote_store_url: Option<String>,
        remote_store_options: Vec<(String, String)>,
        options: ReaderOptions,
    ) -> (
        Self,
        mpsc::Receiver<Arc<CheckpointData>>,
        mpsc::Sender<CheckpointSequenceNumber>,
        oneshot::Sender<()>,
    ) {
        let (checkpoint_sender, checkpoint_recv) = mpsc::channel(MAX_CHECKPOINTS_IN_PROGRESS);
        let (processed_sender, processed_receiver) = mpsc::channel(MAX_CHECKPOINTS_IN_PROGRESS);
        let (exit_sender, exit_receiver) = oneshot::channel();
        let reader = Self {
            path,
            remote_store_url,
            remote_store_options,
            current_checkpoint_number: starting_checkpoint_number,
            last_pruned_watermark: starting_checkpoint_number,
            checkpoint_sender,
            processed_receiver,
            remote_fetcher_receiver: None,
            exit_receiver,
            data_limiter: DataLimiter::new(options.data_limit),
            options,
            historical_reader: None,
            remote_store: None,
        };
        (reader, checkpoint_recv, processed_sender, exit_sender)
    }

    pub async fn run(mut self) -> IngestionResult<()> {
        let (inotify_sender, mut inotify_recv) = mpsc::channel(1);
        std::fs::create_dir_all(self.path.clone()).expect("failed to create a directory");
        let mut watcher = notify::recommended_watcher(move |res| {
            if let Err(err) = res {
                eprintln!("watch error: {:?}", err);
            }
            inotify_sender
                .blocking_send(())
                .expect("Failed to send inotify update");
        })
        .expect("Failed to init inotify");

        watcher
            .watch(&self.path, RecursiveMode::NonRecursive)
            .expect("Inotify watcher failed");
        self.gc_processed_files(self.last_pruned_watermark)
            .expect("Failed to clean the directory");

        let (remote_store, historical_reader) = self.create_remote_store().await;

        self.remote_store = remote_store.map(Arc::new);
        self.historical_reader = historical_reader.map(Arc::new);

        loop {
            tokio::select! {
                _ = &mut self.exit_receiver => break,
                Some(gc_checkpoint_number) = self.processed_receiver.recv() => {
                    self.gc_processed_files(gc_checkpoint_number).expect("Failed to clean the directory");
                }
                Ok(Some(_)) | Err(_) = timeout(Duration::from_millis(self.options.tick_interval_ms), inotify_recv.recv())  => {
                    self.sync().await.expect("Failed to read checkpoint files");
                }
            }
        }
        Ok(())
    }
}

pub struct DataLimiter {
    limit: usize,
    queue: BTreeMap<CheckpointSequenceNumber, usize>,
    in_progress: usize,
}

impl DataLimiter {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            queue: BTreeMap::new(),
            in_progress: 0,
        }
    }

    fn exceeds(&self) -> bool {
        self.limit > 0 && self.in_progress >= self.limit
    }

    fn add(&mut self, checkpoint: &CheckpointData, size: usize) {
        if self.limit == 0 {
            return;
        }
        self.in_progress += size;
        self.queue
            .insert(checkpoint.checkpoint_summary.sequence_number, size);
    }

    fn gc(&mut self, watermark: CheckpointSequenceNumber) {
        if self.limit == 0 {
            return;
        }
        self.queue = self.queue.split_off(&watermark);
        self.in_progress = self.queue.values().sum();
    }
}
