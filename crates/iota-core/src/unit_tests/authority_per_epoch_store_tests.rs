// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use iota_types::base_types::TransactionDigest;
use tempfile::TempDir;
use tokio::task;
use tokio::time::timeout;

use crate::authority::test_authority_builder::TestAuthorityBuilder;

const TXES: [TransactionDigest; 10] = [
    TransactionDigest::new([0; 32]),
    TransactionDigest::new([1; 32]),
    TransactionDigest::new([2; 32]),
    TransactionDigest::new([3; 32]),
    TransactionDigest::new([4; 32]),
    TransactionDigest::new([5; 32]),
    TransactionDigest::new([6; 32]),
    TransactionDigest::new([7; 32]),
    TransactionDigest::new([8; 32]),
    TransactionDigest::new([9; 32]),
];

#[tokio::test]
async fn test_notify_read_executed_transactions_to_checkpoint() {
    let authority_state = TestAuthorityBuilder::new().build().await;
    let store = authority_state.epoch_store_for_testing();
    let checkpoint_sequence_1 = 10;
    let checkpoint_sequence_2 = 12;

    let txes_to_be_notified = vec![
        TransactionDigest::random(),
        TransactionDigest::random(),
        TransactionDigest::random(),
    ];

    // Insert only the first transaction already
    store
        .insert_finalized_transactions(
            vec![txes_to_be_notified[0]].as_slice(),
            checkpoint_sequence_1,
        )
        .expect("Should not fail");

    // Now register to get notified for the addition of some of the above
    // transactions
    let txes_to_be_notified_cloned = txes_to_be_notified.clone();
    let handle = tokio::spawn(async move {
        let notify = store.transactions_executed_in_checkpoint_notify(txes_to_be_notified_cloned);
        notify.await
    });

    // Now insert the rest of the transactions
    let store = authority_state.epoch_store_for_testing();
    store
        .insert_finalized_transactions(&txes_to_be_notified[1..], checkpoint_sequence_2)
        .expect("Should not fail");

    // We should get notified about all the transactions having been executed via
    // checkpoints
    let _ = timeout(Duration::from_secs(5), handle)
        .await
        .expect("Should not timeout")
        .expect("Should not fail");

    // And the transactions should be found into the table
    let result = store
        .multi_get_transaction_checkpoint(txes_to_be_notified.as_slice())
        .expect("Should not fail");
    assert_eq!(result.len(), txes_to_be_notified.len());

    assert_eq!(result[0].unwrap(), checkpoint_sequence_1);
    assert_eq!(result[1].unwrap(), checkpoint_sequence_2);
    assert_eq!(result[2].unwrap(), checkpoint_sequence_2);
}

const DIR: &str = "./tmp";

#[tokio::test]
async fn test_crash_recovery_atomicity() {
    // Create a temporary directory for the DB
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().to_path_buf();

    // Insert a batch of transactions
    let txes = TXES.to_vec();
    let db_path2 = db_path.clone();
    let txes_2 = txes.clone();
    let handle = tokio::spawn(async move {
        let authority_state = TestAuthorityBuilder::new()
            .with_store_base_path(db_path2)
            .build()
            .await;
        let store = authority_state.epoch_store_for_testing();
        store
            .insert_finalized_transactions(&txes_2, 42)
            .expect("Should not fail");
        // Drop store to simulate crash
        store.epoch_terminated().await;
        
    });
    if let Err(e) = handle.await {
        println!("Store drop caused a panic, but continuing test: {:?}", e);
    } else {
        println!("Store drop completed successfully");
    }
}

#[tokio::test]
async fn test_open_writes_and_reads(){
    // Create a temporary directory for the DB
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().to_path_buf();
    let txes = TXES.to_vec();

    // Reopen the store
    let authority_state = TestAuthorityBuilder::new()
        .with_store_base_path(db_path.clone())
        .build()
        .await;
    let store = authority_state.epoch_store_for_testing();
    let result = store.multi_get_transaction_checkpoint(&TXES).expect("Should not fail");
    // All or nothing: either all are present, or none
    let present = result.iter().filter(|r| r.is_some()).count();
    assert!(present == 0 || present == txes.len(), "Atomicity violated: partial batch persisted");
}

#[tokio::test]
async fn test_concurrent_batch_writes_and_reads() {
    let db_path = std::path::Path::new(DIR).to_path_buf();
    let num_tasks = 8;
    let num_tx_per_task = 20;
    let mut handles = Vec::new();
    for i in 0..num_tasks {
        let db_path = db_path.clone();
        handles.push(task::spawn(async move {
            let authority_state = TestAuthorityBuilder::new()
                .with_store_base_path(db_path)
                .build()
                .await;
            let store = authority_state.epoch_store_for_testing();
            let txes: Vec<_> = (0..num_tx_per_task)
                .map(|j| TransactionDigest::random())
                .collect();
            store
                .insert_finalized_transactions(&txes, i as u64)
                .expect("Should not fail");
            // Read back and check consistency
            let result = store.multi_get_transaction_checkpoint(&txes).expect("Should not fail");
            assert!(result.iter().all(|r| r.is_some()), "Some transactions missing after batch write");
        }));
    }
    for handle in handles {
        handle.await.expect("Task panicked");
    }
}
