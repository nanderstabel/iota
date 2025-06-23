// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_const)]

module iota::account;

/// Cannot load an account asset.
/// The address does not have the requested asset.
const EAssetDoesNotExist: u64 = 1;
/// The address has an asset, but the value type does not match.
const EAssetTypeMismatch: u64 = 2;
/// Serialization issue.
const EBCSSerializationFailure: u64 = 3;

/// An account abstraction shared object placeholder.
/// It is not a part of this changes and used just as an example.
public struct Account has key {
    id: UID,
}

/// Creates a new `Account` instance.
public fun create(ctx: &mut TxContext): Account {
    let id = object::new(ctx);
    Account { id }
}

/// Immutably borrows the account-owned asset.
public fun asset<Value: key>(self: &Account, asset: address): &Value {

    // TODO: Ensure the account is unlocked before borrowing the asset.

    borrow_asset<Value>(&self.id, asset)
}

/// Immutably borrows the account-owned asset.
///
/// It is assumed that it is possible to derive the account abstraction address from the `Account`s UID.
///
/// The asset type must has the `key` ability to be accessed from the store.
///
/// # Errors
/// - `EAssetDoesNotExist` if the asset does not exist.
/// - `EAssetTypeMismatch` if the type does not match.
/// - `EBCSSerializationFailure` if serialization fails.
native fun borrow_asset<Value: key>(account: &UID, asset: address): &Value;

// TODO: More assets-related functions can be added here, such as `borrow_mut`, `exists. `remove`, etc.
