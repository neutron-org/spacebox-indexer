CREATE TABLE spacebox.txs_events
(
    `height` Int64,
    `type` String,
    `attributes` String
)
ENGINE = MergeTree
ORDER BY (height,
 type)
SETTINGS index_granularity = 8192;

-- spacebox.txs_events_writer source

CREATE MATERIALIZED VIEW spacebox.txs_events_writer TO spacebox.txs_events
(
    `height` Int64,
    `type` String,
    `attributes` String
) AS
SELECT
    `height`,
    JSONExtractString(`event`, 'type') AS `type`,
    JSONExtractString(`event`, 'attributes') AS `attributes`
FROM
    spacebox.raw_transaction
    ARRAY JOIN (
        JSONExtractArrayRaw(`events`)
    ) as `event`
;

-- spacebox.wasm_txs_events definition

CREATE TABLE spacebox.wasm_txs_events
(
    `timestamp` DateTime,
    `height` Int64,
    `txhash` String,
    `signer` String,
    `contract_address` String,
    `action` String,
    `attributes` String
)
ENGINE = MergeTree
ORDER BY (timestamp,
 height,
 txhash,
 signer,
 contract_address,
 action)
SETTINGS index_granularity = 8192;

-- spacebox.wasm_txs_events_writer source

CREATE MATERIALIZED VIEW spacebox.wasm_txs_events_writer TO spacebox.wasm_txs_events
(

    `timestamp` DateTime,
    `height` Int64,
    `txhash` String,
    `signer` String,
    `contract_address` String,
    `action` String,
    `attributes` String
) AS
SELECT
    `timestamp`,
    `height`,
    `txhash`,
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
    ) AS action,
    JSONExtractString(`event`, 'attributes') as `attributes`
FROM
    spacebox.raw_transaction
    ARRAY JOIN (
        JSONExtractArrayRaw(`events`)
    ) as `event`
WHERE JSONExtractString(`event`, 'type') = 'wasm';
