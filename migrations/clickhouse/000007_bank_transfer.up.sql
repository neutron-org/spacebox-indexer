
-- spacebox.bank_transfer table

CREATE TABLE spacebox.bank_transfer
(
    `timestamp`         DateTime,
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- add coin index so that replacing merge tree keeps all coins of one event
    `coins_index`       Int32,
    -- add computed sort key for easier event ordering
    `sort_key`          Tuple(Int64, Int8, Int32, Int32, Int32)
                        MATERIALIZED tuple(`height`, `block_part_index`, `tx_index`, `event_index`, `coins_index`),
    -- event data
    `type`              LowCardinality(String),
    `address`           String,
    `amount`            Int256,
    `denom`             LowCardinality(String),
    `coins`             String,
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    -- add index for user lookups type queries
    INDEX `address_index` (`address`) TYPE bloom_filter(0.01)
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree()
ORDER BY "sort_key"
SETTINGS index_granularity = 8192;

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
    `amount`            Int256,
    `denom`             LowCardinality(String),
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
        if(
            type = 'coin_spent',
            -toInt128OrZero(extract(`event_coins`, '^(\\d+)')),
            toInt128OrZero(extract(`event_coins`, '^(\\d+)'))
        ) AS `amount`,
        extract(`event_coins`, '^\\d+(.*)') AS `denom`,
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
                        (event_type) -> (
                            event_type = 'coin_spent' OR
                            event_type = 'coin_received'
                        ),
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
    ) AS `event_tuple`
