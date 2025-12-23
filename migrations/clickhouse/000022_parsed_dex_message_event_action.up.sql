
-- spacebox.parsed_dex_message_event_action table

CREATE TABLE spacebox.parsed_dex_message_event_action
(
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- add computed sort key for easier event ordering
    `sort_key`          Tuple(Int64, Int8, Int32, Int32)
                        MATERIALIZED tuple(`height`, `block_part_index`, `tx_index`, `event_index`),
    -- event data
    `type`              LowCardinality(String),
    `action`            LowCardinality(String),
    `attributes_parsed` Array(Tuple(key String, value String, index Bool)),
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    -- add index for action type queries
    INDEX `action_index` (`action`) TYPE set(0)
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree()
ORDER BY (
    -- the minimum unique parts needed to describe a unique Dex event
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`
)
SETTINGS index_granularity = 8192;

-- spacebox.dex_message_parsed_event_action_writer source

CREATE MATERIALIZED VIEW spacebox.dex_message_parsed_event_action_writer TO spacebox.parsed_dex_message_event_action (
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `type`              LowCardinality(String),
    `action`            LowCardinality(String),
    `attributes_parsed` Array(Tuple(key String, value String, index Bool))
) AS
    WITH
        -- define event_tuple parts for row fields
        event_tuple.1 as `event_index`,
        event_tuple.2 as `event_type`,
        event_tuple.3 as `event_attributes`
    SELECT
        `timestamp`,
        `height`,
        `block_part_index`,
        `tx_index`,
        `event_index`,
        `event_type` as `type`,
        -- add event attributes
        tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'action'), `event_attributes`), 2) AS `action`,
        `event_attributes` as `attributes_parsed`
    FROM spacebox.parsed_dex_message_event
    ARRAY JOIN (
        -- Extract "message part" events with event_index
        arrayFlatten(
            arrayMap(
                (msg_part_event, msg_part_event_index) -> arrayMap(
                    (dex_event) -> (
                        -- event_tuple.1: event_index
                        toInt32(`msg_part_events_index_offset` + msg_part_event_index - 1),
                        -- event_tuple.2: event_type
                        tupleElement(dex_event, 1),
                        -- event_tuple.3: event_attributes
                        tupleElement(dex_event, 2)
                    ),
                    -- filter to only possible DEX action events
                    arrayFilter(
                        msg_part_event -> tupleElement(msg_part_event, 1) = 'message',
                        [msg_part_event]
                    )
                ),
                -- enumerate each (msg_part_event, msg_part_event_index) within a message part
                `msg_part_events_parsed`,
                arrayEnumerate(`msg_part_events_parsed`)
            )
        )
    ) AS `event_tuple`
    -- select only DEX actions
    WHERE
        `action` != '' AND
        tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'module'), `event_attributes`), 2) = 'dex'
SETTINGS
    -- allow bigger blocks because transformation is easier
    max_block_size = 1000;
