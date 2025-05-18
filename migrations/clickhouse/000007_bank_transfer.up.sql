
-- spacebox.bank_transfer table

CREATE TABLE spacebox.bank_transfer
(
    `timestamp`         DateTime,
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- add computed sort key for easier event ordering
    `sort_key`          Tuple(Int64, Int8, Int32, Int32)
                        MATERIALIZED tuple(`height`, `block_part_index`, `tx_index`, `event_index`),
    -- add coin index so that replacing merge tree keeps all coins of one event
    `coins_index`       Int32,
    -- event data
    `type`              LowCardinality(String),
    `increment`         Boolean MATERIALIZED `type` = 'coin_received',
    `decrement`         Boolean MATERIALIZED `type` = 'coin_spent',
    `sign`              Int8 MATERIALIZED if(`decrement` = 1, -1, 1),
    `address`           String,
    `denom`             LowCardinality(String),
    `amount`            UInt128,
    `coins`             String,
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    -- add index for user lookups type queries
    INDEX `address_index` (`address`) TYPE bloom_filter(0.01),
    -- add pre-aggregation projections for differently grouped data
    PROJECTION bank_transfer_balance (
        SELECT
            `address`,
            `denom`,
            sum(`amount` * `sign`) as `balance`
        GROUP BY `address`, `denom`
    ),
    PROJECTION bank_transfer_by_minute (
        SELECT
            toStartOfMinute(`timestamp`) as `minute`,
            `address`,
            `denom`,
            sum(`amount` * `sign`) as `amount_delta`
        GROUP BY `address`, `denom`, `minute`
    ),
    PROJECTION bank_transfer_by_day (
        SELECT
            toStartOfDay(`timestamp`) as `day`,
            `address`,
            `denom`,
            sum(`amount` * `sign`) as `amount_delta`
        GROUP BY `address`, `denom`, `day`
    )
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(`timestamp`) -- allows skipping irrelevant months in ReplacingMergeTree merges
ORDER BY (`sort_key`, `coins_index`)
SETTINGS
    -- see docs: https://clickhouse.com/docs/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode
    deduplicate_merge_projection_mode = 'rebuild',
    index_granularity = 8192;


-- spacebox.bank_transfer bank_transfer_balance projection view

CREATE VIEW spacebox.bank_transfer_balance AS
    SELECT
        `address`,
        `denom`,
        sum(`amount` * `sign`) as `balance`
    FROM spacebox.bank_transfer
    GROUP BY `address`, `denom`;

-- spacebox.bank_transfer bank_transfer_by_minute projection view

CREATE VIEW spacebox.bank_transfer_by_minute AS
    SELECT
        toStartOfMinute(`timestamp`) as `minute`,
        `address`,
        `denom`,
        sum(`amount` * `sign`) as `amount_delta`
    FROM spacebox.bank_transfer
    GROUP BY `address`, `denom`, `minute`;

-- spacebox.bank_transfer bank_transfer_by_day projection view

CREATE VIEW spacebox.bank_transfer_by_day AS
    SELECT
        toStartOfDay(`timestamp`) as `day`,
        `address`,
        `denom`,
        sum(`amount` * `sign`) as `amount_delta`
    FROM spacebox.bank_transfer
    GROUP BY `address`, `denom`, `day`;


-- spacebox.bank_transfer_writer source

CREATE MATERIALIZED VIEW spacebox.bank_transfer_writer TO spacebox.bank_transfer (
    `timestamp`         DateTime,
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    `coins_index`       Int32,
    -- event data
    `type`              LowCardinality(String),
    `address`           String,
    `denom`             LowCardinality(String),
    `amount`            UInt128,
    `coins`             String
) AS
    WITH
        -- define event_tuple parts for row fields
        event_tuple.1 as `event_index`,
        event_tuple.2 as `event_type`,
        event_tuple.3 as `event_address`,
        event_tuple.4 as `event_coins`,
        event_tuple.5 as `event_coins_index`
    SELECT
        toDateTime(`timestamp`) as `timestamp`,
        `height`,
        `block_part_index`,
        `tx_index`,
        `event_index`,
        `event_coins_index` as `coins_index`,
        `event_type` as `type`,
        -- add event attributes
        `event_address` as `address`,
        extract(`event_coins`, '^\\d+(.*)') AS `denom`,
        toUInt128OrZero(extract(`event_coins`, '^(\\d+)')) AS `amount`,
        -- append original coin string before parsing
        `event_coins` as `coins`
    FROM spacebox.message_event
    ARRAY JOIN (
        -- Extract "message part" events with event_index
        arrayFlatten(
            arrayMap(
                (msg_event, msg_event_index) -> arrayMap(
                    (event_type) -> arrayMap(
                        (event_attributes) -> arrayMap(
                            (coins_array_string, event_address) -> arrayMap(
                                (coins, coins_index) -> (
                                    -- event_tuple.1: event_index
                                    toInt32(`msg_events_index_offset` + msg_event_index - 1),
                                    -- event_tuple.2: event_type
                                    event_type,
                                    -- event_tuple.3: event_address
                                    event_address,
                                    -- event_tuple.4: event_coins (not that event_coins may be an empty string: = 0 amount)
                                    coins,
                                    -- event_tuple.5: event_coins_index
                                    coins_index
                                ),
                                splitByChar(',', coins_array_string),
                                arrayEnumerate(splitByChar(',', coins_array_string))
                            ),
                            -- extract amount string
                            [JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'amount'), event_attributes), 'value')],
                            -- extract address string
                            [if(
                                event_type = 'coin_spent',
                                JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'spender'), event_attributes), 'value'),
                                JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'receiver'), event_attributes), 'value')
                            )]
                        ),
                        [JSONExtractArrayRaw(msg_event, 'attributes')]
                    ),
                    -- filter to events for table
                    arrayFilter(
                        (event_type) -> event_type in ('coin_spent', 'coin_received'),
                        arrayMap(
                            (msg_event) -> JSONExtractString(msg_event, 'type'),
                            [msg_event]
                        )
                    )
                ),
                -- enumerate each (msg_event, msg_event_index) within a message
                `msg_events`,
                arrayEnumerate(`msg_events`)
            )
        )
    ) AS `event_tuple`;
