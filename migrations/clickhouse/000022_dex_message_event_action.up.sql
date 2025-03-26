
-- spacebox.dex_message_event_action table

CREATE TABLE spacebox.dex_message_event_action
(
    `timestamp`         DateTime,
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `type`              LowCardinality(String),
    `action`            LowCardinality(String),
    `attributes`        String,
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

-- spacebox.dex_message_event_action_writer source

CREATE MATERIALIZED VIEW spacebox.dex_message_event_action_writer TO spacebox.dex_message_event_action (
    `timestamp`         DateTime,
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `type`              LowCardinality(String),
    `action`            LowCardinality(String),
    `attributes`        String
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
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'action'), JSONExtractArrayRaw(`event_attributes`)), 'value') AS `action`,
        `event_attributes` as `attributes`
    FROM spacebox.dex_message_event
    ARRAY JOIN (
        -- Extract "message part" events with event_index
        arrayFlatten(
            arrayMap(
                (msg_part_event, msg_part_event_index) -> arrayMap(
                    (dex_event) -> (
                        -- event_tuple.1: event_index
                        toInt32(`msg_part_events_index_offset` + msg_part_event_index - 1),
                        -- event_tuple.2: event_type
                        JSONExtractString(dex_event, 'type'),
                        -- event_tuple.3: event_attributes
                        JSONExtractString(dex_event, 'attributes')
                    ),
                    -- filter to only possible DEX action events
                    arrayFilter(
                        msg_part_event -> JSONExtractString(msg_part_event, 'type') = 'message',
                        [msg_part_event]
                    )
                ),
                -- enumerate each (msg_part_event, msg_part_event_index) within a message part
                `msg_part_events`,
                arrayEnumerate(`msg_part_events`)
            )
        )
    ) AS `event_tuple`
    -- select only DEX actions
    WHERE
        `action` != '' AND
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'module'), JSONExtractArrayRaw(`event_attributes`)), 'value') = 'dex'
SETTINGS
    -- allow bigger blocks because transformation is easier
    max_block_size = 1000;
