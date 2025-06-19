// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::str::FromStr;

use iota_types::{
    TypeTag,
    base_types::{IotaAddress, ObjectID},
    supported_protocol_versions::Chain,
};
use serde::{Deserialize, Serialize};

use crate::Domain;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct IotaNamesConfig {
    /// Address of the `iota_names` package.
    pub package_address: IotaAddress,
    /// ID of the `IotaNames` object.
    pub object_id: ObjectID,
    /// Address of the `payments` package.
    pub payments_package_address: IotaAddress,
    /// ID of the registry table.
    pub registry_id: ObjectID,
    /// ID of the reverse registry table.
    pub reverse_registry_id: ObjectID,
}

impl Default for IotaNamesConfig {
    fn default() -> Self {
        // TODO change to mainnet https://github.com/iotaledger/iota/issues/6532
        // TODO change to testnet https://github.com/iotaledger/iota/issues/6531
        Self::testnet()
    }
}

impl IotaNamesConfig {
    pub fn new(
        package_address: IotaAddress,
        object_id: ObjectID,
        payments_package_address: IotaAddress,
        registry_id: ObjectID,
        reverse_registry_id: ObjectID,
    ) -> Self {
        Self {
            package_address,
            object_id,
            payments_package_address,
            registry_id,
            reverse_registry_id,
        }
    }

    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self::new(
            std::env::var("IOTA_NAMES_PACKAGE_ADDRESS")?.parse()?,
            std::env::var("IOTA_NAMES_OBJECT_ID")?.parse()?,
            std::env::var("IOTA_NAMES_PAYMENTS_PACKAGE_ADDRESS")?.parse()?,
            std::env::var("IOTA_NAMES_REGISTRY_ID")?.parse()?,
            std::env::var("IOTA_NAMES_REVERSE_REGISTRY_ID")?.parse()?,
        ))
    }

    pub fn from_chain(chain: &Chain) -> Self {
        match chain {
            Chain::Mainnet => todo!("https://github.com/iotaledger/iota/issues/6532"),
            Chain::Testnet => IotaNamesConfig::testnet(),
            Chain::Unknown => IotaNamesConfig::devnet(),
        }
    }

    pub fn record_field_id(&self, domain: &Domain) -> ObjectID {
        let domain_type_tag = Domain::type_(self.package_address);
        let domain_bytes = bcs::to_bytes(domain).unwrap();

        iota_types::dynamic_field::derive_dynamic_field_id(
            self.registry_id,
            &TypeTag::Struct(Box::new(domain_type_tag)),
            &domain_bytes,
        )
        .unwrap()
    }

    pub fn reverse_record_field_id(&self, address: &IotaAddress) -> ObjectID {
        iota_types::dynamic_field::derive_dynamic_field_id(
            self.reverse_registry_id,
            &TypeTag::Address,
            address.as_ref(),
        )
        .unwrap()
    }

    // TODO add mainnet https://github.com/iotaledger/iota/issues/6532

    // TODO add testnet https://github.com/iotaledger/iota/issues/6531

    // Create a config based on the package and object ids published on devnet.
    pub fn devnet() -> Self {
        const PACKAGE_ADDRESS: &str =
            "0x3ec4826f1d6e0d9f00680b2e9a7a41f03788ee610b3d11c24f41ab0ae71da39f";
        const OBJECT_ID: &str =
            "0x54a8a67fad7aa279429e08824e03481dd8b268779353d299d7f8edaa8b8c13b7";
        const PAYMENTS_PACKAGE_ADDRESS: &str =
            "0x882f88d252a650649f490e96e32e53979758fdda645863ca856d83c72d5e0e72";
        const REGISTRY_ID: &str =
            "0xef24c78e8c085e29760d37b287fc16647f0f578e8d22f18dd65f655285afad3e";
        const REVERSE_REGISTRY_ID: &str =
            "0x566dc13eafceaf8c3487ee2c41464553839ef4d50937c63741e359c98080c7b6";

        let package_address = IotaAddress::from_str(PACKAGE_ADDRESS).unwrap();
        let object_id = ObjectID::from_str(OBJECT_ID).unwrap();
        let payments_package_address = IotaAddress::from_str(PAYMENTS_PACKAGE_ADDRESS).unwrap();
        let registry_id = ObjectID::from_str(REGISTRY_ID).unwrap();
        let reverse_registry_id = ObjectID::from_str(REVERSE_REGISTRY_ID).unwrap();

        Self::new(
            package_address,
            object_id,
            payments_package_address,
            registry_id,
            reverse_registry_id,
        )
    }

    pub fn testnet() -> Self {
        const PACKAGE_ADDRESS: &str =
            "0x40932293b0a1521e8dbce8cbb59e8b670fd164e5af3a695083fd92bc8cdc70e6";
        const OBJECT_ID: &str =
            "0xd29e6d2534ef6d96ce1eff25273882ab2b859e5a4f24d1824ea8e5b61ea80407";
        const PAYMENTS_PACKAGE_ADDRESS: &str =
            "0xceb235adc8a762fb3fd908c313975f560f4bcc0a612037607836b1d0595c4872";
        const REGISTRY_ID: &str =
            "0xe70b87ec1ac33f9f9d22a10284d36a2b4769470e54fb2900a4caef669f0c6c82";
        const REVERSE_REGISTRY_ID: &str =
            "0x3b86fd456e008fed61964f737e24cbd58f720e930dd01e1152d0b8ff6a5ee225";

        let package_address = IotaAddress::from_str(PACKAGE_ADDRESS).unwrap();
        let object_id = ObjectID::from_str(OBJECT_ID).unwrap();
        let payments_package_address = IotaAddress::from_str(PAYMENTS_PACKAGE_ADDRESS).unwrap();
        let registry_id = ObjectID::from_str(REGISTRY_ID).unwrap();
        let reverse_registry_id = ObjectID::from_str(REVERSE_REGISTRY_ID).unwrap();

        Self::new(
            package_address,
            object_id,
            payments_package_address,
            registry_id,
            reverse_registry_id,
        )
    }
}
