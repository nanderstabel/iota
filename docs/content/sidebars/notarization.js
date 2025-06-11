// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
const notarization = [
    'iota-notarization/index',
    {
        type: 'category',
        label: 'Getting Started',
        collapsed: false,
        items: [
            'iota-notarization/getting-started/rust',
            'iota-notarization/getting-started/wasm',
            'iota-notarization/getting-started/local-network-setup',
        ],
    },
    {
        type: 'category',
        label: 'Explanations',
        items: [
            'iota-notarization/explanations/about-notarization',
            'iota-notarization/explanations/dynamic-notarization',
            'iota-notarization/explanations/locked-notarization',
            'iota-notarization/explanations/notarization-comparison',
        ],
    },
    {
        type: 'category',
        label: 'How To',
        items: [
            {
                type: 'category',
                label: 'Dynamic Notarizations',
                items: [
                    'iota-notarization/how-tos/dynamic-notarizations/create',
                    'iota-notarization/how-tos/dynamic-notarizations/update-state',
                    'iota-notarization/how-tos/dynamic-notarizations/update-metadata',
                    'iota-notarization/how-tos/dynamic-notarizations/transfer',
                    'iota-notarization/how-tos/dynamic-notarizations/destroy',
                ],
            },
            {
                type: 'category',
                label: 'Locked Notarizations',
                items: [
                    'iota-notarization/how-tos/locked-notarizations/create',
                    'iota-notarization/how-tos/locked-notarizations/destroy',
                ],
            },
            'iota-notarization/how-tos/access-read-only-methods',
        ],
    },
    // {
    //     type: 'category',
    //     label: 'References',
    //     collapsed: true,
    //     items: [
    //         {
    //             type: 'category',
    //             label: 'API',
    //             items: [
    //                 {
    //                     type: 'link',
    //                     label: 'Rust',
    //                     href: 'https://iotaledger.github.io/notarization/notarization/index.html',
    //                 },
    //                 {
    //                     type: 'link',
    //                     label: 'Wasm',
    //                     href: '/references/iota-notarization/wasm/api_ref',
    //                 },
    //             ],
    //         },
    //         {
    //             type: 'category',
    //             label: 'Specifications',
    //             items: [
    //                 'references/iota-notarization/overview',
    //                 'references/iota-notarization/iota-did-method-spec',
    //                 'references/iota-notarization/revocation-bitmap-2022',
    //                 'references/iota-notarization/revocation-timeframe-2024',
    //             ],
    //         },
    //     ],
    // },
    'iota-notarization/contribute',
];

module.exports = notarization;
