CREATE TABLE spacebox.txs_events
(
    `timestamp` DateTime,
    `height` Int64,
    `txhash` String,
    `event_index` Int16,
    `signer` String,
    `type` String,
    `attributes` String
)
ENGINE = ReplacingMergeTree
ORDER BY (
 height,
 txhash,
 event_index
)
SETTINGS index_granularity = 8192;

-- spacebox.txs_events_writer source

CREATE MATERIALIZED VIEW spacebox.txs_events_writer TO spacebox.txs_events
(
    `timestamp` DateTime,
    `height` Int64,
    `txhash` String,
    `event_index` Int16,
    `signer` String,
    `type` String,
    `attributes` String
) AS
SELECT
    toDateTime(`timestamp`) as `timestamp`,
    `height`,
    `txhash`,
    `event_index`,
    `signer`,
    JSONExtractString(`event`, 'type') AS `type`,
    JSONExtractString(`event`, 'attributes') AS `attributes`
FROM
    spacebox.raw_transaction
ARRAY JOIN (JSONExtractArrayRaw(`events`)) as `event`,
    arrayEnumerate(JSONExtractArrayRaw(`events`)) as `event_index`;

-- spacebox.wasm_txs_events definition

CREATE TABLE spacebox.wasm_txs_events
(
    `timestamp` DateTime,
    `height` Int64,
    `txhash` String,
    `event_index` Int16,
    `signer` String,
    `contract_address` String,
    `action` String,
    `attributes` String
)
ENGINE = ReplacingMergeTree
ORDER BY (
 height,
 txhash,
 event_index
)
SETTINGS index_granularity = 8192;

-- spacebox.wasm_txs_events_writer source

CREATE MATERIALIZED VIEW spacebox.wasm_txs_events_writer TO spacebox.wasm_txs_events
(
    `timestamp` DateTime,
    `height` Int64,
    `txhash` String,
    `event_index` Int16,
    `signer` String,
    `contract_address` String,
    `action` String,
    `attributes` String
) AS
SELECT
    `timestamp`,
    `height`,
    `txhash`,
    `event_index`,
    `signer`,
    JSONExtractString(
        arrayFirst(
            x -> (JSONExtractString(x, 'key') = '_contract_address'),
            JSONExtractArrayRaw(`attributes`)
        ),
        'value'
    ) AS `contract_address`,
    JSONExtractString(
        arrayFirst(
            x -> (JSONExtractString(x, 'key') = 'action'),
            JSONExtractArrayRaw(`attributes`)
        ),
        'value'
    ) AS `action`,
    `attributes`
FROM
    spacebox.txs_events
WHERE `type` = 'wasm';
