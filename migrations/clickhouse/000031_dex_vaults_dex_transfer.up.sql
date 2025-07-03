
-- spacebox.dex_vaults_dex_transfer table

CREATE TABLE spacebox.dex_vaults_dex_transfer
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
    `shares_added`      UInt128,
    `shares_removed`    UInt128,
    `shares_total`      UInt128,
    `token_0_from_user` UInt128,
    `token_1_from_user` UInt128,
    `token_0_to_user`   UInt128,
    `token_1_to_user`   UInt128,
    `token_0_from_dex`  UInt128,
    `token_1_from_dex`  UInt128,
    `token_0_to_dex`    UInt128,
    `token_1_to_dex`    UInt128,
    `token_0_balance_before_deposit`   UInt128,
    `token_1_balance_before_deposit`   UInt128,
    `token_0_price`     Float32,
    `token_1_price`     Float32,
    `price_0_to_1`      Float32,
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    -- add index for contract_address type queries
    INDEX `contract_address_index` (`contract_address`) TYPE bloom_filter,
    PROJECTION dex_vaults_dex_transfer_state (
        SELECT
            `contract_address`,
            argMax(`height`, `sort_key`) as `height`,
            -- note: sums are susceptible to double counting
            sum(`token_0_from_user` + `token_0_from_dex` - `token_0_to_user` - `token_1_to_dex`) as `token_0_balance`,
            sum(`token_1_from_user` + `token_1_from_dex` - `token_1_to_user` - `token_1_to_dex`) as `token_1_balance`,
            argMaxIf(`token_0_balance_before_deposit`, `sort_key`, `action` = 'dex_deposit') as `token_0_balance_before_deposit`,
            argMaxIf(`token_1_balance_before_deposit`, `sort_key`, `action` = 'dex_deposit') as `token_1_balance_before_deposit`,
            -- note: sums are susceptible to double counting
            sum(`shares_added` - `shares_removed`) as `shares_total_estimated`,
            argMaxIf(`shares_total`, `sort_key`, `shares_added` + `shares_removed` > 0) as `shares_total`
        GROUP BY `contract_address`
    )
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree()
ORDER BY (`height`, `block_part_index`, `tx_index`, `event_index`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    index_granularity = 8192;


-- spacebox.dex_vaults_dex_transfer dex_vaults_dex_transfer_state projection view

CREATE VIEW spacebox.dex_vaults_dex_transfer_state AS
    SELECT
        `contract_address`,
        argMax(`height`, `sort_key`) as `height`,
        -- note: sums are susceptible to double counting
        sum(`token_0_from_user` + `token_0_from_dex` - `token_0_to_user` - `token_1_to_dex`) as `token_0_balance`,
        sum(`token_1_from_user` + `token_1_from_dex` - `token_1_to_user` - `token_1_to_dex`) as `token_1_balance`,
        argMax(`token_0_balance_before_deposit`, `sort_key`) as `token_0_balance_before_deposit`,
        argMax(`token_1_balance_before_deposit`, `sort_key`) as `token_1_balance_before_deposit`
    FROM spacebox.dex_vaults_dex_transfer
    GROUP BY `contract_address`;


-- spacebox.dex_vaults_dex_transfer_dex_deposit_writer source

CREATE MATERIALIZED VIEW spacebox.dex_vaults_dex_transfer_dex_deposit_writer TO spacebox.dex_vaults_dex_transfer (
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `action`            LowCardinality(String),
    `contract_address`  String,
    `shares_added`      UInt128,
    `shares_removed`    UInt128,
    `shares_total`      UInt128,
    `token_0_from_user` UInt128,
    `token_1_from_user` UInt128,
    `token_0_to_user`   UInt128,
    `token_1_to_user`   UInt128,
    `token_0_from_dex`  UInt128,
    `token_1_from_dex`  UInt128,
    `token_0_to_dex`    UInt128,
    `token_1_to_dex`    UInt128,
    `token_0_balance_before_deposit`   UInt128,
    `token_1_balance_before_deposit`   UInt128,
    `token_0_price`     Float32,
    `token_1_price`     Float32,
    `price_0_to_1`      Float32
) AS
WITH
    toFloat32OrZero(arrayFirst(attr -> attr.key = 'price_0_to_1', `event_attributes`).value) AS `price_ratio`,
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
    arrayFirst(attr -> attr.key = 'action', `event_attributes`).value AS `action`,
    arrayFirst(attr -> attr.key = '_contract_address', `event_attributes`).value AS `contract_address`,
    0 AS `shares_added`,
    0 AS `shares_removed`,
    0 AS `shares_total`,
    0 AS `token_0_from_user`,
    0 AS `token_1_from_user`,
    0 AS `token_0_to_user`,
    0 AS `token_1_to_user`,
    0 AS `token_0_from_dex`,
    0 AS `token_1_from_dex`,
    `deposited_tuple`.1 AS `token_0_to_dex`,
    `deposited_tuple`.2 AS `token_1_to_dex`,
    -- note: token_0/1_balance_before_deposit may be empty (0) in early versions (before price_0_to_1)
    toUInt128OrZero(arrayFirst(attr -> attr.key = 'token_0_balance', `event_attributes`).value) AS `token_0_balance_before_deposit`,
    toUInt128OrZero(arrayFirst(attr -> attr.key = 'token_1_balance', `event_attributes`).value) AS `token_1_balance_before_deposit`,
    toFloat32OrZero(arrayFirst(attr -> attr.key = 'price_0', `event_attributes`).value) AS `token_0_price`,
    toFloat32OrZero(arrayFirst(attr -> attr.key = 'price_1', `event_attributes`).value) AS `token_1_price`,
    if(`token_1_price` > 0, `token_0_price` / `token_1_price`, `price_ratio`) AS `price_0_to_1`
FROM spacebox.parsed_event
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
                                (
                                    arrayFirst(
                                        (attr) -> attr.key = 'ReservesZeroDeposited',
                                        related_message_event_attributes
                                    ).value
                                )
                            ),
                            deposited_tuple.2 + toUInt128OrZero(
                                (
                                    arrayFirst(
                                        (attr) -> attr.key = 'ReservesOneDeposited',
                                        related_message_event_attributes
                                    ).value
                                )
                            )
                        ),
                        arrayFilter(
                            (related_message_event_attributes) -> (
                                (
                                    arrayFirst(
                                        (attr) -> attr.key = 'module',
                                        related_message_event_attributes
                                    ).value
                                ) = 'dex' AND
                                (
                                    arrayFirst(
                                        (attr) -> attr.key = 'action',
                                        related_message_event_attributes
                                    ).value
                                ) = 'DepositLP' AND
                                (
                                    arrayFirst(
                                        (attr) -> attr.key = 'Creator',
                                        related_message_event_attributes
                                    ).value
                                ) = (
                                    arrayFirst(
                                        (attr) -> attr.key = '_contract_address',
                                        msg_event_attributes
                                    ).value
                                )
                            ),
                            arrayMap(
                                (msg_event) -> msg_event.attributes,
                                arrayFilter(
                                    msg_event -> msg_event.type = 'message',
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
                        (
                            arrayFirst(
                                (attr) -> attr.key = 'action',
                                msg_event_attributes
                            ).value
                        ) = 'dex_deposit' AND
                        -- has _contract_address
                        arrayExists(
                            (attr) -> attr.key = '_contract_address',
                            msg_event_attributes
                        )
                    ),
                    arrayMap(
                        (msg_event) -> msg_event.attributes,
                        arrayFilter(
                            msg_event -> msg_event.type = 'wasm',
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
  max_block_size = 10000 -- to height 28000000: Peak memory usage: 94.64 GiB.
;

-- spacebox.dex_vaults_dex_transfer_dex_withdrawal_writer source

CREATE MATERIALIZED VIEW spacebox.dex_vaults_dex_transfer_dex_withdrawal_writer TO spacebox.dex_vaults_dex_transfer (
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `action`            LowCardinality(String),
    `contract_address`  String,
    `shares_added`      UInt128,
    `shares_removed`    UInt128,
    `shares_total`      UInt128,
    `token_0_from_user` UInt128,
    `token_1_from_user` UInt128,
    `token_0_to_user`   UInt128,
    `token_1_to_user`   UInt128,
    `token_0_from_dex`  UInt128,
    `token_1_from_dex`  UInt128,
    `token_0_to_dex`    UInt128,
    `token_1_to_dex`    UInt128,
    `token_0_balance_before_deposit`   UInt128,
    `token_1_balance_before_deposit`   UInt128,
    `token_0_price`     Float32,
    `token_1_price`     Float32,
    `price_0_to_1`      Float32
) AS
WITH
    -- define event_tuple parts for row fields
    event_tuple.1 as `event_index`,
    event_tuple.2 as `event_attributes`,
    event_tuple.3 as `withdrawals_tuple`
SELECT
    `timestamp`,
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`,
    -- add event attributes
    arrayFirst(attr -> attr.key = 'action', `event_attributes`).value AS `action`,
    arrayFirst(attr -> attr.key = '_contract_address', `event_attributes`).value AS `contract_address`,
    0 AS `shares_added`,
    0 AS `shares_removed`,
    0 AS `shares_total`,
    0 AS `token_0_from_user`,
    0 AS `token_1_from_user`,
    0 AS `token_0_to_user`,
    0 AS `token_1_to_user`,
    withdrawals_tuple.1 AS `token_0_from_dex`,
    withdrawals_tuple.2 AS `token_1_from_dex`,
    0 AS `token_0_to_dex`,
    0 AS `token_1_to_dex`,
    0 AS `token_0_balance_before_deposit`,
    0 AS `token_1_balance_before_deposit`,
    0 AS `token_0_price`,
    0 AS `token_1_price`,
    0 AS `price_0_to_1`
FROM spacebox.parsed_event
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
                    -- event_tuple.3: related withdrawals amount
                    arrayFold(
                        (withdrawals_tuple, related_message_event_attributes) -> (
                            withdrawals_tuple.1 + toUInt128OrZero(
                                (
                                    arrayFirst(
                                        (attr) -> attr.key = 'ReservesZeroWithdrawn',
                                        related_message_event_attributes
                                    ).value
                                )
                            ),
                            withdrawals_tuple.2 + toUInt128OrZero(
                                (
                                    arrayFirst(
                                        (attr) -> attr.key = 'ReservesOneWithdrawn',
                                        related_message_event_attributes
                                    ).value
                                )
                            )
                        ),
                        arrayFilter(
                            (related_message_event_attributes) -> (
                                (
                                    arrayFirst(
                                        (attr) -> attr.key = 'module',
                                        related_message_event_attributes
                                    ).value
                                ) = 'dex' AND
                                (
                                    arrayFirst(
                                        (attr) -> attr.key = 'action',
                                        related_message_event_attributes
                                    ).value
                                ) = 'WithdrawLP' AND
                                (
                                    arrayFirst(
                                        (attr) -> attr.key = 'Creator',
                                        related_message_event_attributes
                                    ).value
                                ) = (
                                    arrayFirst(
                                        (attr) -> attr.key = '_contract_address',
                                        msg_event_attributes
                                    ).value
                                )
                            ),
                            arrayMap(
                                (msg_event) -> msg_event.attributes,
                                arrayFilter(
                                    msg_event -> msg_event.type = 'message',
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
                        -- is action="dex_withdrawal"
                        (
                            arrayFirst(
                                (attr) -> attr.key = 'action',
                                msg_event_attributes
                            ).value
                        ) = 'dex_withdrawal' AND
                        -- has _contract_address
                        arrayExists(
                            (attr) -> attr.key = '_contract_address',
                            msg_event_attributes
                        )
                    ),
                    arrayMap(
                        (msg_event) -> msg_event.attributes,
                        arrayFilter(
                            msg_event -> msg_event.type = 'wasm',
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
  max_block_size = 10000
;



-- spacebox.dex_vaults_dex_transfer_user_transfers_writer source

CREATE MATERIALIZED VIEW spacebox.dex_vaults_dex_transfer_user_transfers_writer TO spacebox.dex_vaults_dex_transfer (
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `action`            LowCardinality(String),
    `contract_address`  String,
    `shares_added`      UInt128,
    `shares_removed`    UInt128,
    `shares_total`      UInt128,
    `token_0_from_user` UInt128,
    `token_1_from_user` UInt128,
    `token_0_to_user`   UInt128,
    `token_1_to_user`   UInt128,
    `token_0_from_dex`  UInt128,
    `token_1_from_dex`  UInt128,
    `token_0_to_dex`    UInt128,
    `token_1_to_dex`    UInt128,
    `token_0_balance_before_deposit`   UInt128,
    `token_1_balance_before_deposit`   UInt128,
    `token_0_price`     Float32,
    `token_1_price`     Float32,
    `price_0_to_1`      Float32
) AS
INSERT INTO spacebox.dex_vaults_dex_transfer (
    `timestamp`,
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`,
    -- event data
    `action`,
    `contract_address`,
    `shares_added`,
    `shares_removed`,
    `shares_total`,
    `token_0_from_user`,
    `token_1_from_user`,
    `token_0_to_user`,
    `token_1_to_user`,
    `token_0_from_dex`,
    `token_1_from_dex`,
    `token_0_to_dex`,
    `token_1_to_dex`,
    `token_0_balance_before_deposit`,
    `token_1_balance_before_deposit`,
    `token_0_price`,
    `token_1_price`,
    `price_0_to_1`
)
SELECT
    `timestamp`,
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`,
    -- event data
    `action`,
    `contract_address`,
    `shares_in` AS `shares_added`,
    `shares_out` AS `shares_removed`,
    `total_shares` AS `shares_total`,
    `token_0_deposited` AS `token_0_from_user`,
    `token_1_deposited` AS `token_1_from_user`,
    `token_0_withdrawn` AS `token_0_to_user`,
    `token_1_withdrawn` AS `token_1_to_user`,
    0 AS `token_0_from_dex`,
    0 AS `token_1_from_dex`,
    0 AS `token_0_to_dex`,
    0 AS `token_1_to_dex`,
    0 AS `token_0_balance_before_deposit`,
    0 AS `token_1_balance_before_deposit`,
    0 AS `token_0_price`,
    0 AS `token_1_price`,
    0 AS `price_0_to_1`
FROM spacebox.dex_vaults_shares;
