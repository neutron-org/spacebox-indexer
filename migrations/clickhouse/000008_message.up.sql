-- 000005_message.up.sql

-- spacebox.message definition

CREATE TABLE spacebox.message
(
    `timestamp` DateTime,
    `height` Int64,
    `txhash` String,
    `message_index` Int16,
    `type` String,
    `signer` String,
    `message` String
)
ENGINE = ReplacingMergeTree
ORDER BY (timestamp,
 height,
 txhash,
 message_index,
 type,
 signer)
SETTINGS index_granularity = 8192;

-- spacebox.message_writer source

CREATE MATERIALIZED VIEW spacebox.message_writer TO spacebox.message
(
    `timestamp` DateTime,
    `height` Int64,
    `txhash` String,
    `message_index` Int16,
    `type` String,
    `signer` String,
    `message` String
) AS
SELECT *
FROM
(
    SELECT
        timestamp,
        height,
        txhash,
        arrayJoin(
            arrayEnumerate(
                JSONExtractArrayRaw(
                    JSONExtractString(JSONExtractString(tx, 'body'), 'messages')
                )
            )
        ) AS message_index,
        JSONExtractString(
            arrayJoin(
                JSONExtractArrayRaw(
                    JSONExtractString(JSONExtractString(tx, 'body'), 'messages')
                )
            ),
            '@type'
        ) AS type,
        signer,
        arrayJoin(
            JSONExtractArrayRaw(
                JSONExtractString(JSONExtractString(tx, 'body'), 'messages')
            )
        ) AS message
    FROM spacebox.raw_transaction
    WHERE code = 0
);