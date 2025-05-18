
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
    -- get insert block processed time to mark when data "arrives" in table
    `insert_timestamp`  DateTime DEFAULT nowInBlock(),
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    INDEX `insert_timestamp_index` (`insert_timestamp`) TYPE minmax,
    -- add index for user lookups type queries
    INDEX `address_index` (`address`) TYPE bloom_filter(0.01)
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree(`insert_timestamp`)
PARTITION BY toYYYYMM(`timestamp`) -- allows skipping irrelevant months in ReplacingMergeTree merges
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


-- spacebox.bank_transfer_by_height table

CREATE TABLE spacebox.bank_transfer_by_height
(
    `timestamp`         DateTime,
    `height`            Int64,
    -- event data
    `address`           String,
    `denom`             LowCardinality(String),
    `amount`            Int256,
    `amount_count`      UInt16,
    `sign`              Int8,
    `insert_timestamp`  DateTime,
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    INDEX `insert_timestamp_index` (`insert_timestamp`) TYPE minmax
)
-- use collapsing merge tree to ensure sum of `amount` * `sign` is always the
-- correct total for downstream AggregatingMergeTree tables
ENGINE = CollapsingMergeTree(`sign`)
PARTITION BY toYYYYMM(`timestamp`)
ORDER BY (`address`, `denom`, `height`, `timestamp`)
SETTINGS index_granularity = 8192;

-- spacebox.bank_transfer_by_height_writer source

CREATE MATERIALIZED VIEW spacebox.bank_transfer_by_height_writer
REFRESH EVERY 10 SECOND
APPEND TO spacebox.bank_transfer_by_height (
    `timestamp`         DateTime,
    `height`            Int64,
    -- event data
    `address`           String,
    `denom`             LowCardinality(String),
    `amount`            Int256,
    `amount_count`      UInt16,
    `sign`              Int8,
    `insert_timestamp`  DateTime
)
AS
    WITH
    -- 1) get last processed times from table
    (
        SELECT max(`insert_timestamp`)
        FROM spacebox.bank_transfer_by_height
    ) as `max_processed_timestamp`,
    -- 2) pick up each distinct height that might not yet have been processed
    recent_heights as (
        SELECT DISTINCT `height`
        FROM spacebox.bank_transfer
        WHERE `insert_timestamp` >= `max_processed_timestamp`
    ),
    -- 3) for each height, look up the "old" sum and the "new" sum
    adjustments as (
        SELECT
            new.*,
            coalesce(old.`height`, 0) > 0 AS `has_old_amount`,
            coalesce(old.`old_amount`, 0) AS `old_amount`,
            coalesce(old.`old_amount_count`, 0) AS `old_amount_count`
        FROM (
            WITH if(`type` = 'coin_spent', -`amount`, `amount`) as `amount_delta`
            SELECT
                `timestamp`,
                max(`insert_timestamp`) as `insert_timestamp`,
                `height`,
                `address`,
                `denom`,
                sum(`amount_delta`) AS `new_amount`,
                count(*) AS `new_amount_count`
            FROM spacebox.bank_transfer
            WHERE `height` IN recent_heights
            GROUP BY `height`, `address`, `denom`, `timestamp`
        ) AS new
        LEFT JOIN (
            SELECT
                `height`,
                `address`,
                `denom`,
                sum(`amount` * `sign`) AS `old_amount`,
                count(*) AS `old_amount_count`
            FROM spacebox.bank_transfer_by_height
            WHERE `height` IN recent_heights
            GROUP BY `address`, `denom`, `height`
        ) AS old
        USING (`address`, `denom`, `height`)
    )
    -- 4) for each adjustment: update only if the `amount` needs updating
    SELECT
        `timestamp`,
        `height`,
        -- event data
        `address`,
        `denom`,
        `amount`,
        `amount_count`,
        `sign`,
        `insert_timestamp`
    FROM adjustments
        -- split adjustment into a negative and positive row for the CollapsingMergeTree table
        ARRAY JOIN
            [`old_amount`, `new_amount`] as `amount`,
            [`old_amount_count`, `new_amount_count`] as `amount_count`,
            [-1, 1] as `sign`
    WHERE `new_amount` != `old_amount`
        -- add new rows, but only add negative old rows if one previously existed
        AND (`sign` = 1 OR `has_old_amount` = 1);


-- spacebox.bank_transfer_agg_by_minute_agg table

CREATE TABLE spacebox.bank_transfer_agg_by_minute_agg
(
    `timestamp`         DateTime,
    -- event data
    `address`           String,
    `denom`             LowCardinality(String),
    `amount_state`      AggregateFunction(sum, Int256),
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax
)
-- aggregate to time period for faster windowed queries (sums)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(`timestamp`)
ORDER BY (`address`, `denom`, `timestamp`)
SETTINGS index_granularity = 8192;

-- spacebox.bank_transfer_agg_by_minute_agg_writer source

CREATE MATERIALIZED VIEW spacebox.bank_transfer_agg_by_minute_agg_writer TO spacebox.bank_transfer_agg_by_minute_agg (
    `timestamp`         DateTime,
    `address`           String,
    `denom`             LowCardinality(String),
    `amount_state`      AggregateFunction(sum, Int256)
) AS
    SELECT
        toStartOfInterval(`timestamp`, INTERVAL 1 MINUTE) as `timestamp`,
        `address`,
        sumState(`amount`*`sign`) as `amount_state`,
        `denom`
    FROM spacebox.bank_transfer_by_height
    GROUP BY `address`, `denom`, `timestamp`;

-- spacebox.bank_transfer_by_minute view

CREATE VIEW spacebox.bank_transfer_by_minute AS
    SELECT
        `timestamp`,
        `address`,
        `denom`,
        sumMerge(`amount_state`) as `amount_delta`
    FROM spacebox.bank_transfer_agg_by_minute_agg
    GROUP BY `address`, `denom`, `timestamp`;


-- spacebox.bank_transfer_agg_by_day_agg table

CREATE TABLE spacebox.bank_transfer_agg_by_day_agg
(
    `timestamp`         DateTime,
    -- event data
    `address`           String,
    `denom`             LowCardinality(String),
    `amount_state`      AggregateFunction(sum, Int256),
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax
)
-- aggregate to time period for faster windowed queries (sums)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(`timestamp`)
ORDER BY (`address`, `denom`, `timestamp`)
SETTINGS index_granularity = 8192;

-- spacebox.bank_transfer_agg_by_day_agg_writer source

CREATE MATERIALIZED VIEW spacebox.bank_transfer_agg_by_day_agg_writer TO spacebox.bank_transfer_agg_by_day_agg (
    `timestamp`         DateTime,
    `address`           String,
    `denom`             LowCardinality(String),
    `amount_state`      AggregateFunction(sum, Int256)
) AS
    SELECT
        toStartOfInterval(`timestamp`, INTERVAL 1 DAY) as `timestamp`,
        `address`,
        sumState(`amount`*`sign`) as `amount_state`,
        `denom`
    FROM spacebox.bank_transfer_by_height
    GROUP BY `address`, `denom`, `timestamp`;

-- spacebox.bank_transfer_by_day view

CREATE VIEW spacebox.bank_transfer_by_day AS
    SELECT
        `timestamp`,
        `address`,
        `denom`,
        sumMerge(`amount_state`) as `amount_delta`
    FROM spacebox.bank_transfer_agg_by_day_agg
    GROUP BY `address`, `denom`, `timestamp`;


-- spacebox.bank_transfer_agg_state_agg table

CREATE TABLE spacebox.bank_transfer_agg_state_agg
(
    -- event data
    `address`           String,
    `denom`             LowCardinality(String),
    `amount_state`      AggregateFunction(sum, Int256)
)
-- aggregate to time period for faster windowed queries (sums)
ENGINE = AggregatingMergeTree()
ORDER BY (`address`, `denom`)
SETTINGS index_granularity = 8192;

-- spacebox.bank_transfer_agg_state_agg_writer source

CREATE MATERIALIZED VIEW spacebox.bank_transfer_agg_state_agg_writer TO spacebox.bank_transfer_agg_state_agg (
    `address`           String,
    `denom`             LowCardinality(String),
    `amount_state`      AggregateFunction(sum, Int256)
) AS
    SELECT
        `address`,
        sumState(`amount`*`sign`) as `amount_state`,
        `denom`
    FROM spacebox.bank_transfer_by_height
    GROUP BY `address`, `denom`;

-- spacebox.bank_transfer_state view

CREATE VIEW spacebox.bank_transfer_state AS
    SELECT
        `address`,
        `denom`,
        sumMerge(`amount_state`) as `balance`
    FROM spacebox.bank_transfer_agg_state_agg
    GROUP BY `address`, `denom`;
