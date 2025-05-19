
-- spacebox.dex_shares table

CREATE TABLE spacebox.dex_shares
(
    `timestamp`         DateTime,
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- add computed sort key for easier event ordering
    `sort_key`          Tuple(Int64, Int8, Int32, Int32)
                        MATERIALIZED tuple(`height`, `block_part_index`, `tx_index`, `event_index`),
    -- event data
    `action`            LowCardinality(String),
    `Receiver`          String,
    `TokenZero`         LowCardinality(String),
    `TokenOne`          LowCardinality(String),
    `TickIndex`         Int64,
    `Fee`               UInt64,
    `PoolId`            Int128, -- actually UInt64 but we need -1 for "unsure"
    -- save boolean for credit/debit
    `credit`            Boolean MATERIALIZED `action` = 'DepositLP',
    `shares`            UInt128,
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    -- add index for pool (by attributes) type queries
    INDEX `pool_attributes_index` (`TokenZero`, `TokenOne`, `TickIndex`, `Fee`) TYPE set(0),
    -- add index for pool_id type queries
    INDEX `pool_id_index` (`PoolId`) TYPE set(0)
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree()
ORDER BY `height`, `block_part_index`, `tx_index`, `event_index`
SETTINGS index_granularity = 8192;

-- spacebox.dex_shares_deposit_writer source

CREATE MATERIALIZED VIEW spacebox.dex_shares_deposit_writer TO spacebox.dex_shares (
    `timestamp`         DateTime,
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `action`            LowCardinality(String),
    `Receiver`          String,
    `TokenZero`         LowCardinality(String),
    `TokenOne`          LowCardinality(String),
    `TickIndex`         Int64,
    `Fee`               UInt64,
    `PoolId`            Int128,
    `shares`            UInt128
) AS
    WITH
        -- get shares info from coinbase events (should only be one coinbase event)
        arrayFlatten(
            arrayMap(
                (coinbase_event) -> arrayMap(
                    -- change to tuple to prevent being flattened
                    (coin_parts) -> (
                        -- get the coin string parts corresponding the the DepositLP event
                        -- pool_tuple.1: pool_id
                        toInt128OrZero(coin_parts[2]),
                        -- pool_tuple.2: pool_amount
                        toUInt128OrZero(coin_parts[1])
                    ),
                    arraySort(
                        -- sort by numeric ID in ascending order (originally sorted lexically)
                        (coin_parts) -> coin_parts[2],
                        arrayFilter(
                            (coin_parts) -> (
                                toUInt128OrZero(coin_parts[1]) > 0 AND
                                length(coin_parts) = 2
                            ),
                            arrayMap(
                                -- turn coins string into coin parts [amount, denom]
                                (coins) -> splitByString('neutron/pool/', coins),
                                -- get each coins string from the coinbase amount
                                splitByChar(
                                    ',',
                                    JSONExtractString(
                                        arrayFirst(
                                            (attr) -> JSONExtractString(attr, 'key') = 'amount',
                                            JSONExtractArrayRaw(coinbase_event, 'attributes')
                                        ),
                                        'value'
                                    )
                                )
                            )
                        )
                    )
                ),
                arrayFilter(
                    (msg_part_event) -> JSONExtractString(msg_part_event, 'type') = 'coinbase',
                    `msg_part_events`
                )
            )
        ) as `pool_tuples`,
        -- filter to only non-unique pool_amounts (need to fetch more context)
        arrayFilter(
            (pool_tuple) -> arrayCount((t) -> t.2 = pool_tuple.2, `pool_tuples`) = 1,
            `pool_tuples`
        ) as `unique_pool_tuples`,
        arrayFirst(
            (pool_tuple) -> pool_tuple.2 = `shares`,
            `unique_pool_tuples`
        ) as `found_pool_tuple`,
        -- define event_tuple parts for row fields
        event_tuple.1 as `event_index`,
        event_tuple.2 as `deposit_index`,
        event_tuple.3 as `event_attributes`,
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'PoolId'), `event_attributes`), 'value') AS `maybe_pool_id`
    SELECT
        `timestamp`,
        `height`,
        `block_part_index`,
        `tx_index`,
        `event_index`,
        -- add event attributes
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'action'), `event_attributes`), 'value') AS `action`,
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'Receiver'), `event_attributes`), 'value') AS `Receiver`,
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'TokenZero'), `event_attributes`), 'value') AS `TokenZero`,
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'TokenOne'), `event_attributes`), 'value') AS `TokenOne`,
        toInt64(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'TickIndex'), `event_attributes`), 'value')) AS `TickIndex`,
        toUInt64(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'Fee'), `event_attributes`), 'value')) AS `Fee`,
        if (
            notEmpty(`maybe_pool_id`),
            toInt128(`maybe_pool_id`),
            if (
                found_pool_tuple.2 > 0,
                found_pool_tuple.1,
                toInt128(-1)
            )
        ) AS `PoolId`,
        toUInt128(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'SharesMinted'), `event_attributes`), 'value')) AS `shares`
    FROM spacebox.dex_message_event
    ARRAY JOIN arrayFlatten(
        -- Extract "message part" events with event_index
        arrayMap(
            (msg_part_deposit_events) -> arrayMap(
                (msg_part_deposit_event_index, msg_part_deposit_index) -> (
                    -- event_tuple.1: event_index
                    toInt32(`msg_part_events_index_offset` + msg_part_deposit_event_index - 1),
                    -- event_tuple.2: msg_part_deposit_index
                    msg_part_deposit_index,
                    -- event_tuple.3: event_attributes
                    JSONExtractArrayRaw(`msg_part_events`[msg_part_deposit_event_index], 'attributes')
                ),
                msg_part_deposit_events,
                arrayEnumerate(msg_part_deposit_events)
            ),
            -- get deposit events info
            [arrayFlatten(
                arrayMap(
                    (msg_part_event, msg_part_event_index) -> arrayMap(
                        (dex_deposit_event) -> msg_part_event_index,
                        arrayFilter(
                            (msg_part_event) -> (
                                JSONExtractString(msg_part_event, 'type') = 'message' AND
                                arrayExists(
                                    (attr) -> (
                                        JSONExtractString(attr, 'key') = 'module' AND
                                        JSONExtractString(attr, 'value') = 'dex'
                                    ),
                                    JSONExtractArrayRaw(msg_part_event, 'attributes')
                                ) AND
                                arrayExists(
                                    (attr) -> (
                                        JSONExtractString(attr, 'key') = 'action' AND
                                        JSONExtractString(attr, 'value') = 'DepositLP'
                                    ),
                                    JSONExtractArrayRaw(msg_part_event, 'attributes')
                                )
                            ),
                            [msg_part_event]
                        )
                    ),
                    -- enumerate each (msg_part_event, msg_part_event_index) within a message part
                    `msg_part_events`,
                    arrayEnumerate(`msg_part_events`)
                )
            )]
        )
    ) AS `event_tuple`
SETTINGS
    -- allow bigger blocks because transformation is easier
    max_block_size = 1000;

-- spacebox.dex_shares_withdrawal_writer source

CREATE MATERIALIZED VIEW spacebox.dex_shares_withdrawal_writer TO spacebox.dex_shares (
    `timestamp`         DateTime,
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `action`            LowCardinality(String),
    `Receiver`          String,
    `TokenZero`         LowCardinality(String),
    `TokenOne`          LowCardinality(String),
    `TickIndex`         Int64,
    `Fee`               UInt64,
    `PoolId`            Int128,
    `shares`            UInt128
) AS
    WITH
        -- get shares info from coinbase events (should only be one coinbase event)
        arrayFlatten(
            arrayMap(
                (coinbase_event) -> arrayMap(
                    -- change to tuple to prevent being flattened
                    (coin_parts) -> (
                        -- get the coin string parts corresponding the the DepositLP event
                        -- pool_tuple.1: pool_id
                        toInt128OrZero(coin_parts[2]),
                        -- pool_tuple.2: pool_amount
                        toUInt128OrZero(coin_parts[1])
                    ),
                    arraySort(
                        -- sort by numeric ID in ascending order (originally sorted lexically)
                        (coin_parts) -> coin_parts[2],
                        arrayFilter(
                            (coin_parts) -> (
                                toUInt128OrZero(coin_parts[1]) > 0 AND
                                length(coin_parts) = 2
                            ),
                            arrayMap(
                                -- turn coins string into coin parts [amount, denom]
                                (coins) -> splitByString('neutron/pool/', coins),
                                -- get each coins string from the coinbase amount
                                splitByChar(
                                    ',',
                                    JSONExtractString(
                                        arrayFirst(
                                            (attr) -> JSONExtractString(attr, 'key') = 'amount',
                                            JSONExtractArrayRaw(coinbase_event, 'attributes')
                                        ),
                                        'value'
                                    )
                                )
                            )
                        )
                    )
                ),
                arrayFilter(
                    (msg_part_event) -> JSONExtractString(msg_part_event, 'type') = 'burn',
                    `msg_part_events`
                )
            )
        ) as `pool_tuples`,
        -- filter to only non-unique pool_amounts (need to fetch more context)
        arrayFilter(
            (pool_tuple) -> arrayCount((t) -> t.2 = pool_tuple.2, `pool_tuples`) = 1,
            `pool_tuples`
        ) as `unique_pool_tuples`,
        arrayFirst(
            (pool_tuple) -> pool_tuple.2 = `shares`,
            `unique_pool_tuples`
        ) as `found_pool_tuple`,
        -- define event_tuple parts for row fields
        event_tuple.1 as `event_index`,
        event_tuple.2 as `deposit_index`,
        event_tuple.3 as `event_attributes`,
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'PoolId'), `event_attributes`), 'value') AS `maybe_pool_id`
    SELECT
        `timestamp`,
        `height`,
        `block_part_index`,
        `tx_index`,
        `event_index`,
        -- add event attributes
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'action'), `event_attributes`), 'value') AS `action`,
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'Receiver'), `event_attributes`), 'value') AS `Receiver`,
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'TokenZero'), `event_attributes`), 'value') AS `TokenZero`,
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'TokenOne'), `event_attributes`), 'value') AS `TokenOne`,
        toInt64(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'TickIndex'), `event_attributes`), 'value')) AS `TickIndex`,
        toUInt64(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'Fee'), `event_attributes`), 'value')) AS `Fee`,
        if (
            notEmpty(`maybe_pool_id`),
            toInt128(`maybe_pool_id`),
            if (
                found_pool_tuple.2 > 0,
                found_pool_tuple.1,
                toInt128(-1)
            )
        ) AS `PoolId`,
        toUInt128(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'SharesRemoved'), `event_attributes`), 'value')) AS `shares`
    FROM spacebox.dex_message_event
    ARRAY JOIN arrayFlatten(
        -- Extract "message part" events with event_index
        arrayMap(
            (msg_part_deposit_events) -> arrayMap(
                (msg_part_deposit_event_index, msg_part_deposit_index) -> (
                    -- event_tuple.1: event_index
                    toInt32(`msg_part_events_index_offset` + msg_part_deposit_event_index - 1),
                    -- event_tuple.2: msg_part_deposit_index
                    msg_part_deposit_index,
                    -- event_tuple.3: event_attributes
                    JSONExtractArrayRaw(`msg_part_events`[msg_part_deposit_event_index], 'attributes')
                ),
                msg_part_deposit_events,
                arrayEnumerate(msg_part_deposit_events)
            ),
            -- get deposit events info
            [arrayFlatten(
                arrayMap(
                    (msg_part_event, msg_part_event_index) -> arrayMap(
                        (dex_deposit_event) -> msg_part_event_index,
                        arrayFilter(
                            (msg_part_event) -> (
                                JSONExtractString(msg_part_event, 'type') = 'message' AND
                                arrayExists(
                                    (attr) -> (
                                        JSONExtractString(attr, 'key') = 'module' AND
                                        JSONExtractString(attr, 'value') = 'dex'
                                    ),
                                    JSONExtractArrayRaw(msg_part_event, 'attributes')
                                ) AND
                                arrayExists(
                                    (attr) -> (
                                        JSONExtractString(attr, 'key') = 'action' AND
                                        JSONExtractString(attr, 'value') = 'WithdrawLP'
                                    ),
                                    JSONExtractArrayRaw(msg_part_event, 'attributes')
                                )
                            ),
                            [msg_part_event]
                        )
                    ),
                    -- enumerate each (msg_part_event, msg_part_event_index) within a message part
                    `msg_part_events`,
                    arrayEnumerate(`msg_part_events`)
                )
            )]
        )
    ) AS `event_tuple`
SETTINGS
    -- allow bigger blocks because transformation is easier
    max_block_size = 1000;

-- update unknown DEX pool metadata using raw_dex_pool_metadata until data appears on the share events themselves

CREATE MATERIALIZED VIEW spacebox.dex_shares_pool_id_writer TO spacebox.dex_shares (
    `timestamp`         DateTime,
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `action`            LowCardinality(String),
    `Receiver`          String,
    `TokenZero`         LowCardinality(String),
    `TokenOne`          LowCardinality(String),
    `TickIndex`         Int64,
    `Fee`               UInt64,
    `PoolId`            Int128,
    `shares`            UInt128
) AS
    SELECT
        shares.`timestamp` as `timestamp`,
        shares.`height` as `height`,
        shares.`block_part_index` as `block_part_index`,
        shares.`tx_index` as `tx_index`,
        shares.`event_index` as `event_index`,
        shares.`action` as `action`,
        shares.`Receiver` as `Receiver`,
        shares.`TokenZero` as `TokenZero`,
        shares.`TokenOne` as `TokenOne`,
        shares.`TickIndex` as `TickIndex`,
        shares.`Fee` as `Fee`,
        metadata.`id` as `PoolId`,
        shares.`shares` as `shares`
    FROM spacebox.raw_dex_pool_metadata as metadata
    -- only add if fix is not yet applied
    INNER JOIN (
        SELECT `applied`
        FROM spacebox.fixes
        WHERE `id` = 2
        AND `applied` = 0
    ) as fix ON 1=1
    INNER JOIN (
        -- need all fields to be able to overwrite these
        SELECT
            `timestamp`,
            `height`,
            `block_part_index`,
            `tx_index`,
            `event_index`,
            `action`,
            `Receiver`,
            `TokenZero`,
            `TokenOne`,
            `TickIndex`,
            `Fee`,
            `shares`
        FROM spacebox.dex_shares
        WHERE `PoolId` = -1
    ) as shares
    ON metadata.`token0` = shares.`TokenZero`
    AND metadata.`token1` = shares.`TokenOne`
    AND metadata.`tick` = shares.`TickIndex`
    AND metadata.`fee` = shares.`Fee`
