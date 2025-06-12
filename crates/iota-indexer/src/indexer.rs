// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, env, path::PathBuf, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use iota_data_ingestion_core::{
    DataIngestionMetrics, IndexerExecutor, ProgressStore, ReaderOptions, WorkerPool,
};
use iota_metrics::spawn_monitored_task;
use iota_types::messages_checkpoint::CheckpointSequenceNumber;
use prometheus::Registry;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    IndexerConfig, build_json_rpc_server,
    errors::IndexerError,
    handlers::{
        Handler,
        checkpoint_handler::new_handlers,
        objects_snapshot_handler::{
            ObjectsSnapshotHandler, SnapshotLagConfig, start_objects_snapshot_handler,
        },
        pruner::Pruner,
    },
    indexer_reader::IndexerReader,
    metrics::IndexerMetrics,
    processors::processor_orchestrator::ProcessorOrchestrator,
    store::{IndexerAnalyticalStore, IndexerStore, PgIndexerStore},
};

pub(crate) const DOWNLOAD_QUEUE_SIZE: usize = 200;
const INGESTION_READER_TIMEOUT_SECS: u64 = 20;
// Limit indexing parallelism on big checkpoints to avoid OOM,
// by limiting the total size of batch checkpoints to ~20MB.
// On testnet, most checkpoints are < 200KB, some can go up to 50MB.
const CHECKPOINT_PROCESSING_BATCH_DATA_LIMIT: usize = 20000000;

pub struct Indexer;

impl Indexer {
    pub async fn start_writer(
        config: &IndexerConfig,
        store: PgIndexerStore,
        metrics: IndexerMetrics,
    ) -> Result<(), IndexerError> {
        let snapshot_config = SnapshotLagConfig::default();
        Indexer::start_writer_with_config(
            config,
            store,
            metrics,
            snapshot_config,
            None,
            CancellationToken::new(),
        )
        .await
    }

    pub async fn start_writer_with_config(
        config: &IndexerConfig,
        store: PgIndexerStore,
        metrics: IndexerMetrics,
        snapshot_config: SnapshotLagConfig,
        epochs_to_keep: Option<u64>,
        cancel: CancellationToken,
    ) -> Result<(), IndexerError> {
        info!(
            "IOTA Indexer Writer (version {:?}) started...",
            env!("CARGO_PKG_VERSION")
        );

        let primary_watermark = store
            .get_latest_checkpoint_sequence_number()
            .await
            .expect("Failed to get latest tx checkpoint sequence number from DB")
            .map(|seq| seq + 1)
            .unwrap_or_default();
        let download_queue_size = env::var("DOWNLOAD_QUEUE_SIZE")
            .unwrap_or_else(|_| DOWNLOAD_QUEUE_SIZE.to_string())
            .parse::<usize>()
            .expect("Invalid DOWNLOAD_QUEUE_SIZE");
        let ingestion_reader_timeout_secs = env::var("INGESTION_READER_TIMEOUT_SECS")
            .unwrap_or_else(|_| INGESTION_READER_TIMEOUT_SECS.to_string())
            .parse::<u64>()
            .expect("Invalid INGESTION_READER_TIMEOUT_SECS");
        let data_limit = std::env::var("CHECKPOINT_PROCESSING_BATCH_DATA_LIMIT")
            .unwrap_or(CHECKPOINT_PROCESSING_BATCH_DATA_LIMIT.to_string())
            .parse::<usize>()
            .unwrap();
        let extra_reader_options = ReaderOptions {
            batch_size: download_queue_size,
            timeout_secs: ingestion_reader_timeout_secs,
            data_limit,
            ..Default::default()
        };

        // Start objects snapshot processor, which is a separate pipeline with its
        // ingestion pipeline.
        let (object_snapshot_worker, object_snapshot_watermark) = start_objects_snapshot_handler(
            store.clone(),
            metrics.clone(),
            snapshot_config,
            cancel.clone(),
        )
        .await?;

        let epochs_to_keep = epochs_to_keep.or_else(|| {
            std::env::var("EPOCHS_TO_KEEP")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
        });
        if let Some(epochs_to_keep) = epochs_to_keep {
            info!(
                "Starting indexer pruner with epochs to keep: {}",
                epochs_to_keep
            );
            assert!(epochs_to_keep > 0, "Epochs to keep must be positive");
            let pruner: Pruner = Pruner::new(store.clone(), epochs_to_keep, metrics.clone())?;
            spawn_monitored_task!(pruner.start(CancellationToken::new()));
        }

        // If we already have chain identifier indexed (i.e. the first checkpoint has
        // been indexed), then we persist protocol configs for protocol versions
        // not yet in the db. Otherwise, we would do the persisting in
        // `commit_checkpoint` while the first cp is being indexed.
        if let Some(chain_id) = IndexerStore::get_chain_identifier(&store).await? {
            store.persist_protocol_configs_and_feature_flags(chain_id)?;
        }
        // Ingestion task watermarks are snapshotted once on indexer startup based on
        // the corresponding watermark table before being handed off to the
        // ingestion task.
        let progress_store = ShimIndexerProgressStore::new(vec![
            ("primary".to_string(), primary_watermark),
            ("object_snapshot".to_string(), object_snapshot_watermark),
        ]);

        let mut primary_executor = IndexerExecutor::new(
            progress_store.clone(),
            2,
            DataIngestionMetrics::new(&Registry::new()),
            cancel.child_token(),
        );
        let worker = new_handlers(store, metrics, primary_watermark, cancel.clone()).await?;
        let worker_pool = WorkerPool::new(
            worker,
            "primary".to_string(),
            download_queue_size,
            Default::default(),
        );
        primary_executor.register(worker_pool).await?;

        match (
            config.data_ingestion_path.as_ref(),
            config.remote_store_url.as_ref(),
        ) {
            (None, None) => {
                return Err(IndexerError::Generic(
                    "provide an ingestion path and/or a remote store url".into(),
                ));
            }
            (None, Some(remote_url)) => {
                // Case 1: ingestion-path is None, run two executors (both use remote-url and
                // tempdir)
                let ingestion_path = tempfile::tempdir().unwrap().into_path();
                let object_snapshot_executor_handle = Self::spawn_object_snapshot_executor_task(
                    object_snapshot_worker,
                    progress_store,
                    cancel.child_token(),
                    download_queue_size,
                    ingestion_path.clone(),
                    remote_url.clone(),
                    extra_reader_options.clone(),
                );
                primary_executor
                    .run(
                        ingestion_path,
                        Some(remote_url.clone()),
                        vec![],
                        extra_reader_options,
                    )
                    .await?;
                object_snapshot_executor_handle.await??;
            }
            (Some(ingestion_path), Some(remote_url)) => {
                // Case 2: remote-url is Some, run two executors
                // - primary uses ingestion-path
                // - object snapshot uses remote-url and tempdir
                let object_snapshot_executor_handle = Self::spawn_object_snapshot_executor_task(
                    object_snapshot_worker,
                    progress_store,
                    cancel.child_token(),
                    download_queue_size,
                    tempfile::tempdir().unwrap().into_path(),
                    remote_url.clone(),
                    extra_reader_options.clone(),
                );
                primary_executor
                    .run(ingestion_path.clone(), None, vec![], extra_reader_options)
                    .await?;
                object_snapshot_executor_handle.await??;
            }
            (Some(ingestion_path), None) => {
                println!("case 3");
                // Case 3: only ingestion-path is set, run a single executor with both worker
                // pools
                let worker_pool = WorkerPool::new(
                    object_snapshot_worker,
                    "object_snapshot".to_string(),
                    download_queue_size,
                    Default::default(),
                );
                primary_executor.register(worker_pool).await?;
                primary_executor
                    .run(ingestion_path.clone(), None, vec![], extra_reader_options)
                    .await?;
            }
        }
        Ok(())
    }

    fn spawn_object_snapshot_executor_task(
        object_snapshot_worker: ObjectsSnapshotHandler,
        progress_store: ShimIndexerProgressStore,
        cancel: CancellationToken,
        concurrency: usize,
        ingestion_path: PathBuf,
        remote_store: String,
        reader_options: ReaderOptions,
    ) -> JoinHandle<Result<(), IndexerError>> {
        tokio::spawn(async move {
            // This loop waits until there is at least one committable
            // checkpoint before launching the executor.
            loop {
                let should_start_object_snapshot_executor = object_snapshot_worker
                    .get_max_committable_checkpoint()
                    .await
                    .map(|seq| seq != 0)
                    .unwrap_or_default();

                if should_start_object_snapshot_executor {
                    tracing::info!("starting obj executor");
                    break;
                }
                tokio::time::sleep(Duration::from_secs(5 * 60)).await; // 5 min
            }

            let mut executor = IndexerExecutor::new(
                progress_store,
                1,
                DataIngestionMetrics::new(&Registry::new()),
                cancel.child_token(),
            );
            let worker_pool = WorkerPool::new(
                object_snapshot_worker,
                "object_snapshot".to_string(),
                concurrency,
                Default::default(),
            );
            executor.register(worker_pool).await?;
            executor
                .run(ingestion_path, Some(remote_store), vec![], reader_options)
                .await
                .map(|_| ())
                .map_err(Into::into)
        })
    }

    pub async fn start_reader(
        config: &IndexerConfig,
        registry: &Registry,
        db_url: String,
    ) -> Result<(), IndexerError> {
        info!(
            "IOTA Indexer Reader (version {:?}) started...",
            env!("CARGO_PKG_VERSION")
        );
        let indexer_reader = IndexerReader::new(db_url)?;
        let handle = build_json_rpc_server(registry, indexer_reader, config, None)
            .await
            .expect("Json rpc server should not run into errors upon start.");
        tokio::spawn(async move { handle.stopped().await })
            .await
            .expect("Rpc server task failed");

        Ok(())
    }
    pub async fn start_analytical_worker<
        S: IndexerAnalyticalStore + Clone + Send + Sync + 'static,
    >(
        store: S,
        metrics: IndexerMetrics,
    ) -> Result<(), IndexerError> {
        info!(
            "IOTA Indexer Analytical Worker (version {:?}) started...",
            env!("CARGO_PKG_VERSION")
        );
        let mut processor_orchestrator = ProcessorOrchestrator::new(store, metrics);
        processor_orchestrator.run_forever().await;
        Ok(())
    }
}

#[derive(Clone)]
struct ShimIndexerProgressStore {
    watermarks: HashMap<String, CheckpointSequenceNumber>,
}

impl ShimIndexerProgressStore {
    fn new(watermarks: Vec<(String, CheckpointSequenceNumber)>) -> Self {
        Self {
            watermarks: watermarks.into_iter().collect(),
        }
    }
}

#[async_trait]
impl ProgressStore for ShimIndexerProgressStore {
    type Error = IndexerError;

    async fn load(&mut self, task_name: String) -> Result<CheckpointSequenceNumber, Self::Error> {
        Ok(*self.watermarks.get(&task_name).expect("missing watermark"))
    }

    async fn save(&mut self, _: String, _: CheckpointSequenceNumber) -> Result<(), Self::Error> {
        Ok(())
    }
}
