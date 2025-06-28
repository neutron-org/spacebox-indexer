
-- spacebox.dex_vaults_dex_balance table

CREATE TABLE spacebox.dex_vaults_dex_balance
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
    `action`            LowCardinality(String),
    `contract_address`  String,
    `token_0_balance`   UInt128,
    `token_1_balance`   UInt128,
    `token_0_balance_before_deposit`   UInt128,
    `token_1_balance_before_deposit`   UInt128,
    `token_0_price`     Float32,
    `token_1_price`     Float32,
    `price_0_to_1`      Float32,
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    -- add index for contract_address type queries
    INDEX `contract_address_index` (`contract_address`) TYPE bloom_filter,
    PROJECTION dex_vaults_dex_balance_state (
        SELECT
            `contract_address`,
            argMax(`height`, `sort_key`) as `height`,
            argMax(`token_0_balance`, `sort_key`) as `token_0_balance`,
            argMax(`token_1_balance`, `sort_key`) as `token_1_balance`,
            argMax(`token_0_balance_before_deposit`, `sort_key`) as `token_0_balance_before_deposit`,
            argMax(`token_1_balance_before_deposit`, `sort_key`) as `token_1_balance_before_deposit`
        GROUP BY `contract_address`
    )
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree()
ORDER BY (`height`, `block_part_index`, `tx_index`, `event_index`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    index_granularity = 8192;


-- spacebox.dex_vaults_dex_balance dex_vaults_dex_balance_state projection view

CREATE VIEW spacebox.dex_vaults_dex_balance_state AS
    SELECT
        `contract_address`,
        argMax(`height`, `sort_key`) as `height`,
        argMax(`token_0_balance`, `sort_key`) as `token_0_balance`,
        argMax(`token_1_balance`, `sort_key`) as `token_1_balance`,
        argMax(`token_0_balance_before_deposit`, `sort_key`) as `token_0_balance_before_deposit`,
        argMax(`token_1_balance_before_deposit`, `sort_key`) as `token_1_balance_before_deposit`
    FROM spacebox.dex_vaults_dex_balance
    GROUP BY `contract_address`;


-- spacebox.dex_vaults_dex_balance_deposit_writer source

CREATE MATERIALIZED VIEW spacebox.dex_vaults_dex_balance_deposit_writer TO spacebox.dex_vaults_dex_balance (
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `action`            LowCardinality(String),
    `contract_address`  String,
    `token_0_balance`   UInt128,
    `token_1_balance`   UInt128,
    `token_0_balance_before_deposit`   UInt128,
    `token_1_balance_before_deposit`   UInt128,
    `token_0_price`     Float32,
    `token_1_price`     Float32,
    `price_0_to_1`      Float32
) AS
WITH
    toFloat32OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'price_0_to_1'), `event_attributes`), 'value')) AS `price_ratio`,
    -- define event_tuple parts for row fields
    event_tuple.1 as `event_index`,
    event_tuple.2 as `event_attributes`,
    event_tuple.3 as `deposited_tuple`
SELECT
    `timestamp`,
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`,
    -- add event attributes
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'action'), `event_attributes`), 'value') AS `action`,
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = '_contract_address'), `event_attributes`), 'value') AS `contract_address`,
    `deposited_tuple`.1 AS `token_0_balance`,
    `deposited_tuple`.2 AS `token_1_balance`,
    toUInt128OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'token_0_balance'), `event_attributes`), 'value')) AS `token_0_balance_before_deposit`,
    toUInt128OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'token_1_balance'), `event_attributes`), 'value')) AS `token_1_balance_before_deposit`,
    toFloat32OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'price_0'), `event_attributes`), 'value')) AS `token_0_price`,
    toFloat32OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'price_1'), `event_attributes`), 'value')) AS `token_1_price`,
    if(`token_1_price` > 0, `token_0_price` / `token_1_price`, `price_ratio`) AS `price_0_to_1`
FROM spacebox.message_event
ARRAY JOIN (
    -- Extract "message part" events with event_index
    arrayFlatten(
        arrayMap(
            (msg_event, msg_event_index) -> arrayMap(
                (msg_event_attributes) -> (
                    -- event_tuple.1: event_index
                    toInt32(`msg_events_index_offset` + msg_event_index - 1),
                    -- event_tuple.2: event_attributes
                    msg_event_attributes,
                    -- event_tuple.3: related deposited amount
                    arrayFold(
                        (deposited_tuple, related_message_event_attributes) -> (
                            deposited_tuple.1 + toUInt128OrZero(
                                JSONExtractString(
                                    arrayFirst(
                                        (attr) -> JSONExtractString(attr, 'key') = 'ReservesZeroDeposited',
                                        related_message_event_attributes
                                    ),
                                    'value'
                                )
                            ),
                            deposited_tuple.2 + toUInt128OrZero(
                                JSONExtractString(
                                    arrayFirst(
                                        (attr) -> JSONExtractString(attr, 'key') = 'ReservesOneDeposited',
                                        related_message_event_attributes
                                    ),
                                    'value'
                                )
                            )
                        ),
                        arrayFilter(
                            (related_message_event_attributes) -> (
                                JSONExtractString(
                                    arrayFirst(
                                        (attr) -> JSONExtractString(attr, 'key') = 'module',
                                        related_message_event_attributes
                                    ),
                                    'value'
                                ) = 'dex' AND
                                JSONExtractString(
                                    arrayFirst(
                                        (attr) -> JSONExtractString(attr, 'key') = 'action',
                                        related_message_event_attributes
                                    ),
                                    'value'
                                ) = 'DepositLP' AND
                                JSONExtractString(
                                    arrayFirst(
                                        (attr) -> JSONExtractString(attr, 'key') = 'Creator',
                                        related_message_event_attributes
                                    ),
                                    'value'
                                ) = JSONExtractString(
                                    arrayFirst(
                                        (attr) -> JSONExtractString(attr, 'key') = '_contract_address',
                                        msg_event_attributes
                                    ),
                                    'value'
                                )
                            ),
                            arrayMap(
                                (msg_event) -> JSONExtractArrayRaw(msg_event, 'attributes'),
                                arrayFilter(
                                    msg_event -> JSONExtractString(msg_event, 'type') = 'message',
                                    `msg_events`
                                )
                            )
                        ),
                        (toUInt128(0), toUInt128(0))
                    )
                ),
                -- filter to only successful execution events
                arrayFilter(
                    (msg_event_attributes) -> (
                        -- is action="dex_deposit"
                        JSONExtractString(
                            arrayFirst(
                                (attr) -> JSONExtractString(attr, 'key') = 'action',
                                msg_event_attributes
                            ),
                            'value'
                        ) = 'dex_deposit' AND
                        -- note: token_0/1_balance is the balance of the vault before depositing to the DEX
                        --       the 'dex_deposit' action may try to deposit all of these tokens however
                        --       it does not account for "swap on deposit" or potential errors (dropped deposit events)
                        -- has token_0_balance
                        arrayExists(
                            (attr) -> JSONExtractString(attr, 'key') = 'token_0_balance',
                            msg_event_attributes
                        ) AND
                        -- has token_1_balance
                        arrayExists(
                            (attr) -> JSONExtractString(attr, 'key') = 'token_1_balance',
                            msg_event_attributes
                        ) AND
                        -- has _contract_address
                        arrayExists(
                            (attr) -> JSONExtractString(attr, 'key') = '_contract_address',
                            msg_event_attributes
                        )
                    ),
                    arrayMap(
                        (msg_event) -> JSONExtractArrayRaw(msg_event, 'attributes'),
                        arrayFilter(
                            msg_event -> JSONExtractString(msg_event, 'type') = 'wasm',
                            [msg_event]
                        )
                    )
                )
            ),
            -- enumerate each (msg_event, msg_event_index) within a message part
            `msg_events`,
            arrayEnumerate(`msg_events`)
        )
    )
) AS `event_tuple`
SETTINGS
  -- this query can have trouble backfilling with a lot of blocks
  max_insert_block_size = 10000 -- to height 25697698: Peak memory usage: 94.64 GiB.
;

-- spacebox.dex_vaults_dex_balance_withdrawal_writer source

CREATE MATERIALIZED VIEW spacebox.dex_vaults_dex_balance_withdrawal_writer TO spacebox.dex_vaults_dex_balance (
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `action`            LowCardinality(String),
    `contract_address`  String,
    `token_0_balance`   UInt128,
    `token_1_balance`   UInt128,
    `token_0_balance_before_deposit`   UInt128,
    `token_1_balance_before_deposit`   UInt128,
    `token_0_price`     Float32,
    `token_1_price`     Float32,
    `price_0_to_1`      Float32
) AS
WITH
    -- define event_tuple parts for row fields
    event_tuple.1 as `event_index`,
    event_tuple.2 as `event_attributes`
SELECT
    `timestamp`,
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`,
    -- add event attributes
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'action'), `event_attributes`), 'value') AS `action`,
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = '_contract_address'), `event_attributes`), 'value') AS `contract_address`,
    0 AS `token_0_balance`,
    0 AS `token_1_balance`,
    0 AS `token_0_balance_before_deposit`,
    0 AS `token_1_balance_before_deposit`,
    0 AS `token_0_price`,
    0 AS `token_1_price`,
    0 AS `price_0_to_1`
FROM spacebox.message_event
ARRAY JOIN (
    -- Extract "message part" events with event_index
    arrayFlatten(
        arrayMap(
            (msg_event, msg_event_index) -> arrayMap(
                (msg_event_attributes) -> (
                    -- event_tuple.1: event_index
                    toInt32(`msg_events_index_offset` + msg_event_index - 1),
                    -- event_tuple.2: event_attributes
                    msg_event_attributes
                ),
                -- filter to only successful execution events
                arrayFilter(
                    (msg_event_attributes) -> (
                        -- is action="dex_withdrawal"
                        JSONExtractString(
                            arrayFirst(
                                (attr) -> JSONExtractString(attr, 'key') = 'action',
                                msg_event_attributes
                            ),
                            'value'
                        ) = 'dex_withdrawal' AND
                        -- has _contract_address
                        arrayExists(
                            (attr) -> JSONExtractString(attr, 'key') = '_contract_address',
                            msg_event_attributes
                        )
                    ),
                    arrayMap(
                        (msg_event) -> JSONExtractArrayRaw(msg_event, 'attributes'),
                        arrayFilter(
                            msg_event -> JSONExtractString(msg_event, 'type') = 'wasm',
                            [msg_event]
                        )
                    )
                )
            ),
            -- enumerate each (msg_event, msg_event_index) within a message part
            `msg_events`,
            arrayEnumerate(`msg_events`)
        )
    )
) AS `event_tuple`
SETTINGS
  -- this query can have trouble backfilling with a lot of blocks
  max_insert_block_size = 10000
;
