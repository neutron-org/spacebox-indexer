
-- spacebox.dex_vaults_config_parts table

CREATE TABLE spacebox.dex_vaults_config_tx_event
(
    `timestamp`                 DateTime,
    `height`                    Int64,
    `txhash`                    String,
    `event_index`               Int16,
    `signer`                    String,
    `contract_address`          String,
    `action`                    String,
    `attributes`                String,
    -- add index for quick joins when finding related configs
    INDEX `contract_address_index` (`contract_address`) TYPE bloom_filter
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree()
ORDER BY (
    `height`,
    `txhash`,
    `event_index`
)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW spacebox.dex_vaults_config_tx_event_writer TO spacebox.dex_vaults_config_tx_event (
    `timestamp`                 DateTime,
    `height`                    Int64,
    `txhash`                    String,
    `event_index`               Int16 DEFAULT 0,
    `signer`                    String,
    `contract_address`          String,
    `action`                    String,
    `attributes`                String
) AS
SELECT
    `timestamp`,
    `height`,
    `txhash`,
    `event_index`,
    `signer`,
    `contract_address`,
    `action`,
    `attributes`
FROM spacebox.wasm_txs_events
WHERE `action` in (
    'instantiate IMM',
    'update_config',
    'create_token'
)
