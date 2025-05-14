
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
    `address`           String,
    `amount`            UInt128,
    `denom`             LowCardinality(String),
    `coins`             String,
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    -- add index for user lookups type queries
    INDEX `address_index` (`address`) TYPE bloom_filter(0.01)
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree()
ORDER BY (`sort_key`, `coins_index`)
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
        toUInt128OrZero(extract(`event_coins`, '^(\\d+)')) AS `amount`,
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


-- spacebox.bank_transfer_by_height table

CREATE TABLE spacebox.bank_transfer_by_height
(
    `timestamp`         DateTime,
    `height`            Int64,
    -- event data
    `address`           String,
    `amount_state`      AggregateFunction(sum, Int256),
    `denom`             LowCardinality(String),
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax
)
-- aggregate to block height for faster windowed queries (sums)
ENGINE = AggregatingMergeTree()
ORDER BY (`address`, `denom`, `height`)
SETTINGS index_granularity = 8192;

-- spacebox.bank_transfer_by_height_writer source

CREATE MATERIALIZED VIEW spacebox.bank_transfer_by_height_writer TO spacebox.bank_transfer_by_height (
    `timestamp`         DateTime,
    `height`            Int64,
    `address`           String,
    `amount_state`      AggregateFunction(sum, Int256),
    `denom`             LowCardinality(String)
) AS
    WITH
        if(`type` = 'coin_spent', -`amount`, `amount`) as `amount_delta`
    SELECT
        any(`timestamp`) as `timestamp`,
        `height`,
        `address`,
        sumState(`amount_delta`) as `amount_state`,
        `denom`
    FROM spacebox.bank_transfer
    GROUP BY `address`, `denom`, `height`
    -- ignore zero-sum withdrawal-then-deposit same amount during block behavior
    HAVING sum(`amount_delta`) != 0;


-- spacebox.bank_transfer_by_minute table

CREATE TABLE spacebox.bank_transfer_by_minute
(
    `timestamp`         DateTime,
    -- event data
    `address`           String,
    `amount_state`      AggregateFunction(sum, Int256),
    `denom`             LowCardinality(String),
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax
)
-- aggregate to time period for faster windowed queries (sums)
ENGINE = AggregatingMergeTree()
ORDER BY (`address`, `denom`, `timestamp`)
SETTINGS index_granularity = 8192;

-- spacebox.bank_transfer_by_minute_writer source

CREATE MATERIALIZED VIEW spacebox.bank_transfer_by_minute_writer TO spacebox.bank_transfer_by_minute (
    `timestamp`         DateTime,
    `address`           String,
    `amount_state`      AggregateFunction(sum, Int256),
    `denom`             LowCardinality(String)
) AS
    SELECT
        toStartOfInterval(`timestamp`, INTERVAL 1 MINUTE) as `timestamp`,
        `address`,
        sumMergeState(`amount_state`) as `amount_state`,
        `denom`
    FROM spacebox.bank_transfer_by_height
    GROUP BY `address`, `denom`, `timestamp`;


-- spacebox.bank_transfer_by_day table

CREATE TABLE spacebox.bank_transfer_by_day
(
    `timestamp`         DateTime,
    -- event data
    `address`           String,
    `amount_state`      AggregateFunction(sum, Int256),
    `denom`             LowCardinality(String),
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax
)
-- aggregate to time period for faster windowed queries (sums)
ENGINE = AggregatingMergeTree()
ORDER BY (`address`, `denom`, `timestamp`)
SETTINGS index_granularity = 8192;

-- spacebox.bank_transfer_by_day_writer source

CREATE MATERIALIZED VIEW spacebox.bank_transfer_by_day_writer TO spacebox.bank_transfer_by_day (
    `timestamp`         DateTime,
    `address`           String,
    `amount_state`      AggregateFunction(sum, Int256),
    `denom`             LowCardinality(String)
) AS
    SELECT
        toStartOfInterval(`timestamp`, INTERVAL 1 DAY) as `timestamp`,
        `address`,
        sumMergeState(`amount_state`) as `amount_state`,
        `denom`
    FROM spacebox.bank_transfer_by_height
    GROUP BY `address`, `denom`, `timestamp`;


-- spacebox.bank_transfer_state table

CREATE TABLE spacebox.bank_transfer_state
(
    `timestamp_state`   AggregateFunction(max, DateTime),
    -- event data
    `address`           String,
    `amount_state`      AggregateFunction(sum, Int256),
    `denom`             LowCardinality(String),
)
-- aggregate to user denom for faster user denom state lookups
ENGINE = AggregatingMergeTree()
ORDER BY (`address`, `denom`)
SETTINGS index_granularity = 8192;

-- spacebox.bank_transfer_state_writer source

CREATE MATERIALIZED VIEW spacebox.bank_transfer_state_writer TO spacebox.bank_transfer_state (
    `timestamp_state`   AggregateFunction(max, DateTime),
    `address`           String,
    `amount_state`      AggregateFunction(sum, Int256),
    `denom`             LowCardinality(String)
) AS
    SELECT
        maxState(`timestamp`) as `timestamp_state`,
        `address`,
        sumMergeState(`amount_state`) as `amount_state`,
        `denom`
    FROM spacebox.bank_transfer_by_height
    GROUP BY `address`, `denom`;
