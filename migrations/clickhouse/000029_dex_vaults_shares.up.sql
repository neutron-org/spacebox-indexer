
-- spacebox.dex_vaults_shares table

CREATE TABLE spacebox.dex_vaults_shares
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
    `creator`           String,
    `action`            LowCardinality(String),
    `contract_address`  String,
    -- save boolean for credit/debit
    `credit`            Boolean MATERIALIZED `action` = 'deposit',
    `token_0_deposited` UInt128, -- amount deposited
    `token_1_deposited` UInt128, -- amount deposited
    `token_0_withdrawn` UInt128, -- amount withdrawn
    `token_1_withdrawn` UInt128, -- amount withdrawn
    `shares_in`         UInt128, -- shares added
    `shares_out`        UInt128, -- shares removed
    `total_shares`      UInt128, -- total shares
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    -- add index for contract_address type queries
    INDEX `contract_address_index` (`contract_address`) TYPE bloom_filter,
    PROJECTION dex_vaults_shares_state (
        SELECT
            argMax(`timestamp`, `sort_key`) as `timestamp`,
            argMax(`height`, `sort_key`) as `height`,
            `contract_address`,
            argMax(`total_shares`, `sort_key`) as `shares`
        GROUP BY `contract_address`
    )
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree()
ORDER BY (`height`, `block_part_index`, `tx_index`, `event_index`)
SETTINGS
    index_granularity = 8192;


-- spacebox.dex_vaults_shares dex_vaults_shares_state projection view

CREATE VIEW spacebox.dex_vaults_shares_state AS
    SELECT
        argMax(`timestamp`, `sort_key`) as `timestamp`,
        argMax(`height`, `sort_key`) as `height`,
        `contract_address`,
        argMax(`total_shares`, `sort_key`) as `shares`
    FROM spacebox.dex_vaults_shares
    GROUP BY `contract_address`;


-- spacebox.preparsed_dex_vaults_shares_deposit_writer source

CREATE MATERIALIZED VIEW spacebox.preparsed_dex_vaults_shares_deposit_writer TO spacebox.dex_vaults_shares (
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `creator`           String,
    `action`            LowCardinality(String),
    `contract_address`  String,
    `token_0_deposited` UInt128,
    `token_1_deposited` UInt128,
    `token_0_withdrawn` UInt128,
    `token_1_withdrawn` UInt128,
    `shares_in`         UInt128,
    `shares_out`        UInt128,
    `total_shares`      UInt128
) AS
WITH
    -- define event_tuple parts for row fields
    event_tuple.1 as `event_index`,
    event_tuple.2 as `related_message_event_attributes`,
    event_tuple.3 as `event_attributes`
SELECT
    `timestamp`,
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`,
    -- add event attributes
    tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'sender'), `related_message_event_attributes`), 2) AS `creator`,
    tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'action'), `event_attributes`), 2) AS `action`,
    tupleElement(arrayFirst(x -> (tupleElement(x, 1) = '_contract_address'), `event_attributes`), 2) AS `contract_address`,
    toUInt128OrZero(tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'token_0_deposited'), `event_attributes`), 2)) AS `token_0_deposited`,
    toUInt128OrZero(tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'token_1_deposited'), `event_attributes`), 2)) AS `token_1_deposited`,
    0 AS `token_0_withdrawn`,
    0 AS `token_1_withdrawn`,
    toUInt128OrZero(tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'minted_amount'), `event_attributes`), 2)) AS `shares_in`,
    0 AS `shares_out`,
    toUInt128OrZero(tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'total_shares'), `event_attributes`), 2)) AS `total_shares`
FROM spacebox.parsed_event
ARRAY JOIN (
    -- Extract "message part" events with event_index
    arrayFlatten(
        arrayMap(
            (msg_tf_mint_event_attributes) -> arrayMap(
                (msg_event_parsed, msg_event_index) -> arrayMap(
                    (msg_event_attributes) -> (
                        -- event_tuple.1: event_index
                        toInt32(`msg_events_index_offset` + msg_event_index - 1),
                        -- event_tuple.2: related message event attributes
                        tupleElement(
                            arrayFirst(
                                (msg_event_parsed) -> (
                                    msg_event_parsed.1 = 'message' AND
                                    arrayExists(
                                        (attr) -> (
                                            tupleElement(attr, 1) = 'action' AND
                                            tupleElement(attr, 2) = '/cosmwasm.wasm.v1.MsgExecuteContract'
                                        ),
                                        msg_event_parsed.2
                                    )
                                ),
                                `msg_events_parsed`
                            ),
                            2
                        ),
                        -- event_tuple.3: event_attributes
                        msg_event_attributes
                    ),
                    -- filter to only successful execution events
                    arrayFilter(
                        (msg_event_attributes_parsed) -> (
                            -- is action="deposit"
                            tupleElement(
                                arrayFirst(
                                    (attr) -> attr.1 = 'action',
                                    msg_event_attributes_parsed
                                ),
                                2
                            ) = 'deposit' AND
                            -- has total_shares>0
                            toUInt128OrZero(
                                tupleElement(
                                    arrayFirst(
                                        (attr) -> attr.1 = 'total_shares',
                                        msg_event_attributes_parsed
                                    ),
                                    2
                                )
                            ) > 0 AND
                            -- matches tf_mint event contract address
                            has(
                                [
                                    tupleElement(
                                        arrayFirst(
                                            (attr) -> attr.1 = '_contract_address',
                                            msg_event_attributes_parsed
                                        ),
                                        2
                                    ),
                                    tupleElement(
                                        arrayFirst(
                                            (attr) -> attr.1 = 'from',
                                            msg_event_attributes_parsed
                                        ),
                                        2
                                    )
                                ],
                                tupleElement(
                                    arrayFirst(
                                        (attr) -> attr.1 = 'mint_to_address',
                                        msg_tf_mint_event_attributes
                                    ),
                                    2
                                )
                            ) AND
                            -- matches tf_mint event amount
                            tupleElement(
                                arrayFirst(
                                    (attr) -> attr.1 = 'minted_amount',
                                    msg_event_attributes_parsed
                                ),
                                2
                            ) = regexpExtract(
                                tupleElement(
                                    arrayFirst(
                                        (attr) -> attr.1 = 'amount',
                                        msg_tf_mint_event_attributes
                                    ),
                                    2
                                ),
                                '^(\\d+)'
                            )
                        ),
                        arrayMap(
                            (msg_event_parsed) -> msg_event_parsed.2,
                            arrayFilter(
                                msg_event_parsed -> msg_event_parsed.1 = 'wasm',
                                [msg_event_parsed]
                            )
                        )
                    )
                ),
                -- enumerate each (msg_event_parsed, msg_event_index) within a message part
                `msg_events_parsed`,
                arrayEnumerate(`msg_events_parsed`)
            ),
            -- get tf_mint event attributes (if it exists)
            arrayMap(
                (msg_event_parsed) -> msg_event_parsed.2,
                arrayFilter(
                    (msg_event_parsed) -> msg_event_parsed.1 = 'tf_mint',
                    `msg_events_parsed`
                )
            )
        )
    )
) AS `event_tuple`
WHERE notEmpty(`creator`);


-- spacebox.preparsed_dex_vaults_shares_withdrawal_writer source

CREATE MATERIALIZED VIEW spacebox.preparsed_dex_vaults_shares_withdrawal_writer TO spacebox.dex_vaults_shares (
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `creator`           String,
    `action`            LowCardinality(String),
    `contract_address`  String,
    `token_0_deposited` UInt128,
    `token_1_deposited` UInt128,
    `token_0_withdrawn` UInt128,
    `token_1_withdrawn` UInt128,
    `shares_in`         UInt128,
    `shares_out`        UInt128,
    `total_shares`      UInt128
) AS
WITH
    -- define event_tuple parts for row fields
    event_tuple.1 as `event_index`,
    event_tuple.2 as `related_message_event_attributes`,
    event_tuple.3 as `event_attributes`
SELECT
    `timestamp`,
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`,
    -- add event attributes
    tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'sender'), `related_message_event_attributes`), 2) AS `creator`,
    -- note: save "withdrawal_reply_success" as "withdrawal"
    'withdrawal' AS `action`,
    tupleElement(arrayFirst(x -> (tupleElement(x, 1) = '_contract_address'), `event_attributes`), 2) AS `contract_address`,
    0 AS `token_0_deposited`,
    0 AS `token_1_deposited`,
    toUInt128OrZero(tupleElement(arrayFirst(x -> (tupleElement(x, 1) IN ('withdraw_amount_0', 'withdrawn_token_0')), `event_attributes`), 2)) AS `token_0_withdrawn`,
    toUInt128OrZero(tupleElement(arrayFirst(x -> (tupleElement(x, 1) IN ('withdraw_amount_1', 'withdrawn_token_1')), `event_attributes`), 2)) AS `token_1_withdrawn`,
    0 AS `shares_in`,
    toUInt128OrZero(tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'shares_burned'), `event_attributes`), 2)) AS `shares_out`,
    toUInt128OrZero(tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'total_shares'), `event_attributes`), 2)) AS `total_shares`
FROM spacebox.parsed_event
ARRAY JOIN (
    -- Extract "message part" events with event_index
    arrayFlatten(
        arrayMap(
            (msg_tf_burn_event_attributes) -> arrayMap(
                (msg_event, msg_event_index) -> arrayMap(
                    (msg_event_attributes) -> (
                        -- event_tuple.1: event_index
                        toInt32(`msg_events_index_offset` + msg_event_index - 1),
                        -- event_tuple.2: related message event attributes
                        tupleElement(
                            arrayFirst(
                                (msg_event) -> (
                                    tupleElement(msg_event, 1) = 'message' AND
                                    arrayExists(
                                        (attr) -> (
                                            tupleElement(attr, 1) = 'action' AND
                                            tupleElement(attr, 2) = '/cosmwasm.wasm.v1.MsgExecuteContract'
                                        ),
                                        tupleElement(msg_event, 2)
                                    )
                                ),
                                `msg_events_parsed`
                            ),
                            2
                        ),
                        -- event_tuple.3: event_attributes
                        msg_event_attributes
                    ),
                    -- filter to only successful execution events
                    arrayFilter(
                        (msg_event_attributes) -> (
                            -- is action="withdrawal"
                            tupleElement(
                                arrayFirst(
                                    (attr) -> tupleElement(attr, 1) = 'action',
                                    msg_event_attributes
                                ),
                                2
                            ) IN ('withdrawal', 'withdrawal_reply_success') AND
                            -- has shares_burned>0
                            toUInt128OrZero(
                                tupleElement(
                                    arrayFirst(
                                        (attr) -> tupleElement(attr, 1) = 'shares_burned',
                                        msg_event_attributes
                                    ),
                                    2
                                )
                            ) > 0 AND
                            -- matches tf_burn event contract address
                            tupleElement(
                                arrayFirst(
                                    (attr) -> tupleElement(attr, 1) = '_contract_address',
                                    msg_event_attributes
                                ),
                                2
                            ) = tupleElement(
                                arrayFirst(
                                    (attr) -> tupleElement(attr, 1) = 'burn_from_address',
                                    msg_tf_burn_event_attributes
                                ),
                                2
                            ) AND
                            -- matches tf_burn event amount
                            tupleElement(
                                arrayFirst(
                                    (attr) -> tupleElement(attr, 1) = 'shares_burned',
                                    msg_event_attributes
                                ),
                                2
                            ) = regexpExtract(
                                tupleElement(
                                    arrayFirst(
                                        (attr) -> tupleElement(attr, 1) = 'amount',
                                        msg_tf_burn_event_attributes
                                    ),
                                    2
                                ),
                                '^(\\d+)'
                            )
                        ),
                        arrayMap(
                            (msg_event) -> tupleElement(msg_event, 2),
                            arrayFilter(
                                msg_event -> tupleElement(msg_event, 1) = 'wasm',
                                [msg_event]
                            )
                        )
                    )
                ),
                -- enumerate each (msg_event, msg_event_index) within a message part
                `msg_events_parsed`,
                arrayEnumerate(`msg_events_parsed`)
            ),
            -- get tf_burn event attributes (if it exists)
            arrayMap(
                (msg_event) -> tupleElement(msg_event, 2),
                arrayFilter(
                    (msg_event) -> tupleElement(msg_event, 1) = 'tf_burn',
                    `msg_events_parsed`
                )
            )
        )
    )
) AS `event_tuple`
WHERE notEmpty(`creator`);
