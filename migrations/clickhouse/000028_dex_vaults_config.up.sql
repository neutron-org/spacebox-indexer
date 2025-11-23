
-- spacebox.dex_vaults_config_parts table

CREATE TABLE spacebox.dex_vaults_config_event
(
    `timestamp`                 DateTime64(9),
    `height`                    Int64,
    `block_part_index`          Int8,
    `tx_index`                  Int32,
    `event_index`               Int32,
    -- add computed sort key for easier event ordering
    `sort_key`                  Tuple(Int64, Int8, Int32, Int32)
                                MATERIALIZED tuple(`height`, `block_part_index`, `tx_index`, `event_index`),
    -- event data
    `attributes`                String,
    `contract_address`          String,
    `action`                    String,
    -- the attributes here attempt to follow the contract types
    -- link: https://github.com/neutron-org/slinky-vault/blob/a8843298fdf794eacf3667ca9843297072c662d7/contracts/mmvault/src/msg.rs#L30-L56
    `whitelist`                 Nullable(String), -- JSON of Array(String): often in events as "owner"
    `token_0_denom`             Nullable(String),
    `token_1_denom`             Nullable(String),
    `token_0_symbol`            Nullable(String),
    `token_1_symbol`            Nullable(String),
    `token_0_quote_currency`    Nullable(String),
    `token_1_quote_currency`    Nullable(String),
    `token_0_decimals`          Nullable(UInt8),
    `token_1_decimals`          Nullable(UInt8),
    `token_0_max_blocks_old`    Nullable(UInt64), -- often in events as "max_blocks_stale_token_a"
    `token_1_max_blocks_old`    Nullable(UInt64), -- often in events as "max_blocks_stale_token_b"
    `pool_id`                   Nullable(String),
    `deposit_cap`               Nullable(UInt128),
    `timestamp_stale`           Nullable(UInt64),
    `fee_tier_config`           Nullable(String), -- JSON of FeeTiers Array: (fee: u64, percentage: u64)
    `paused`                    Nullable(Boolean),
    `skew`                      Nullable(Boolean),
    `imbalance`                 Nullable(UInt32),
    `oracle_contract`           Nullable(String),
    `oracle_price_skew`         Nullable(Int32),
     -- only set by "create_token" action
    `denom`                     Nullable(String),
     -- add projection to get current state quickly
    PROJECTION dex_vaults_config_state (
        SELECT
            argMin(`timestamp`, `sort_key`) as `created_at`,
            argMax(`timestamp`, `sort_key`) as `updated_at`,
            argMin(`height`, `sort_key`) as `created_at_height`,
            argMax(`height`, `sort_key`) as `updated_at_height`,
            `contract_address`,
            argMax(`whitelist`, `sort_key`) as `whitelist`,
            argMax(`token_0_denom`, `sort_key`) as `token_0_denom`,
            argMax(`token_1_denom`, `sort_key`) as `token_1_denom`,
            argMax(`token_0_symbol`, `sort_key`) as `token_0_symbol`,
            argMax(`token_1_symbol`, `sort_key`) as `token_1_symbol`,
            argMax(`token_0_quote_currency`, `sort_key`) as `token_0_quote_currency`,
            argMax(`token_1_quote_currency`, `sort_key`) as `token_1_quote_currency`,
            argMax(`token_0_decimals`, `sort_key`) as `token_0_decimals`,
            argMax(`token_1_decimals`, `sort_key`) as `token_1_decimals`,
            argMax(`token_0_max_blocks_old`, `sort_key`) as `token_0_max_blocks_old`,
            argMax(`token_1_max_blocks_old`, `sort_key`) as `token_1_max_blocks_old`,
            argMax(`pool_id`, `sort_key`) as `pool_id`,
            argMax(`deposit_cap`, `sort_key`) as `deposit_cap`,
            argMax(`timestamp_stale`, `sort_key`) as `timestamp_stale`,
            argMax(`fee_tier_config`, `sort_key`) as `fee_tier_config`,
            argMax(`paused`, `sort_key`) as `paused`,
            argMax(`skew`, `sort_key`) as `skew`,
            argMax(`imbalance`, `sort_key`) as `imbalance`,
            argMax(`oracle_contract`, `sort_key`) as `oracle_contract`,
            argMax(`oracle_price_skew`, `sort_key`) as `oracle_price_skew`,
            argMax(`denom`, `sort_key`) as `denom`,
            (
                `denom` IS NOT NULL AND
                `token_0_denom` IS NOT NULL AND
                `token_1_denom` IS NOT NULL AND
                `fee_tier_config` IS NOT NULL
            ) as `is_valid`
        GROUP BY `contract_address`
    )
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree()
ORDER BY (
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`
)
SETTINGS
    -- see docs: https://clickhouse.com/docs/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode
    deduplicate_merge_projection_mode = 'rebuild',
    index_granularity = 8192;


-- spacebox.dex_vaults_shares dex_vaults_config_state projection view

CREATE VIEW spacebox.dex_vaults_config_state AS
    SELECT *
    FROM (
        SELECT
            argMin(`timestamp`, `sort_key`) as `created_at`,
            argMax(`timestamp`, `sort_key`) as `updated_at`,
            argMin(`height`, `sort_key`) as `created_at_height`,
            argMax(`height`, `sort_key`) as `updated_at_height`,
            `contract_address`,
            argMax(`whitelist`, `sort_key`) as `whitelist`,
            argMax(`token_0_denom`, `sort_key`) as `token_0_denom`,
            argMax(`token_1_denom`, `sort_key`) as `token_1_denom`,
            argMax(`token_0_symbol`, `sort_key`) as `token_0_symbol`,
            argMax(`token_1_symbol`, `sort_key`) as `token_1_symbol`,
            argMax(`token_0_quote_currency`, `sort_key`) as `token_0_quote_currency`,
            argMax(`token_1_quote_currency`, `sort_key`) as `token_1_quote_currency`,
            argMax(`token_0_decimals`, `sort_key`) as `token_0_decimals`,
            argMax(`token_1_decimals`, `sort_key`) as `token_1_decimals`,
            argMax(`token_0_max_blocks_old`, `sort_key`) as `token_0_max_blocks_old`,
            argMax(`token_1_max_blocks_old`, `sort_key`) as `token_1_max_blocks_old`,
            argMax(`pool_id`, `sort_key`) as `pool_id`,
            argMax(`deposit_cap`, `sort_key`) as `deposit_cap`,
            argMax(`timestamp_stale`, `sort_key`) as `timestamp_stale`,
            argMax(`fee_tier_config`, `sort_key`) as `fee_tier_config`,
            argMax(`paused`, `sort_key`) as `paused`,
            argMax(`skew`, `sort_key`) as `skew`,
            argMax(`imbalance`, `sort_key`) as `imbalance`,
            argMax(`oracle_contract`, `sort_key`) as `oracle_contract`,
            argMax(`oracle_price_skew`, `sort_key`) as `oracle_price_skew`,
            argMax(`denom`, `sort_key`) as `denom`,
            (
                `denom` IS NOT NULL AND
                `token_0_denom` IS NOT NULL AND
                `token_1_denom` IS NOT NULL AND
                `fee_tier_config` IS NOT NULL
            ) as `is_valid`
            FROM spacebox.dex_vaults_config_event
            GROUP BY `contract_address`
    )
    WHERE `is_valid` = 1
    ORDER BY `contract_address` ASC;

-- spacebox.dex_vaults_shares dex_vaults_by_pair_config_state projection view

CREATE VIEW spacebox.dex_vaults_by_pair_config_state AS
    SELECT *
    FROM (
        WITH s.`created_at_height` as `sort_key`
        SELECT
            argMax(`created_at`, `sort_key`) as `created_at`,
            argMax(`updated_at`, `sort_key`) as `updated_at`,
            argMax(`created_at_height`, `sort_key`) as `created_at_height`,
            argMax(`updated_at_height`, `sort_key`) as `updated_at_height`,
            argMax(`contract_address`, `sort_key`) as `contract_address`,
            argMax(`whitelist`, `sort_key`) as `whitelist`,
            `token_0_denom`,
            `token_1_denom`,
            argMax(`token_0_symbol`, `sort_key`) as `token_0_symbol`,
            argMax(`token_1_symbol`, `sort_key`) as `token_1_symbol`,
            argMax(`token_0_quote_currency`, `sort_key`) as `token_0_quote_currency`,
            argMax(`token_1_quote_currency`, `sort_key`) as `token_1_quote_currency`,
            argMax(`token_0_decimals`, `sort_key`) as `token_0_decimals`,
            argMax(`token_1_decimals`, `sort_key`) as `token_1_decimals`,
            argMax(`token_0_max_blocks_old`, `sort_key`) as `token_0_max_blocks_old`,
            argMax(`token_1_max_blocks_old`, `sort_key`) as `token_1_max_blocks_old`,
            argMax(`pool_id`, `sort_key`) as `pool_id`,
            argMax(`deposit_cap`, `sort_key`) as `deposit_cap`,
            argMax(`timestamp_stale`, `sort_key`) as `timestamp_stale`,
            argMax(`fee_tier_config`, `sort_key`) as `fee_tier_config`,
            argMax(`paused`, `sort_key`) as `paused`,
            argMax(`skew`, `sort_key`) as `skew`,
            argMax(`imbalance`, `sort_key`) as `imbalance`,
            argMax(`oracle_contract`, `sort_key`) as `oracle_contract`,
            argMax(`oracle_price_skew`, `sort_key`) as `oracle_price_skew`,
            argMax(`denom`, `sort_key`) as `denom`
            FROM spacebox.dex_vaults_config_state as s
            GROUP BY `token_0_denom`, `token_1_denom`
    )
    ORDER BY `contract_address` ASC;


-- spacebox.preparsed_dex_vaults_config_event_writer source

CREATE MATERIALIZED VIEW spacebox.preparsed_dex_vaults_config_event_writer TO spacebox.dex_vaults_config_event (
    `timestamp`                 DateTime64(9),
    `height`                    Int64,
    `block_part_index`          Int8,
    `tx_index`                  Int32,
    `event_index`               Int32,
    -- event data
    `attributes`                String,
    `contract_address`          String,
    `action`                    String,
    -- the attributes here attempt to follow the contract types
    -- link: https://github.com/neutron-org/slinky-vault/blob/a8843298fdf794eacf3667ca9843297072c662d7/contracts/mmvault/src/msg.rs#L30-L56
    `whitelist`                 Nullable(String), -- often in events as "owner"
    `token_0_denom`             Nullable(String),
    `token_1_denom`             Nullable(String),
    `token_0_symbol`            Nullable(String),
    `token_1_symbol`            Nullable(String),
    `token_0_quote_currency`    Nullable(String),
    `token_1_quote_currency`    Nullable(String),
    `token_0_decimals`          Nullable(UInt8),
    `token_1_decimals`          Nullable(UInt8),
    `token_0_max_blocks_old`    Nullable(UInt64), -- often in events as "max_blocks_stale_token_a"
    `token_1_max_blocks_old`    Nullable(UInt64), -- often in events as "max_blocks_stale_token_b"
    `pool_id`                   Nullable(String),
    `deposit_cap`               Nullable(UInt128),
    `timestamp_stale`           Nullable(UInt64),
    `fee_tier_config`           Nullable(String), -- JSON of FeeTiers Array: (fee: u64, percentage: u64)
    `paused`                    Nullable(Boolean),
    `skew`                      Nullable(Boolean),
    `imbalance`                 Nullable(UInt32),
    `oracle_contract`           Nullable(String),
    `oracle_price_skew`         Nullable(Int32),
     -- only set by "create_token" action
    `denom`                     Nullable(String)
) AS
WITH
    -- define event_tuple parts for row fields
    event_tuple.1 as `event_index`,
    event_tuple.2 as `event_attributes_parsed`,
    event_tuple.3 as `attributes`,
    (arrayFirst(attr -> (attr.1 = '_contract_address'), `event_attributes_parsed`)).2 AS `contract_address`,
    (arrayFirst(attr -> (attr.1 = 'action'), `event_attributes_parsed`)).2 AS `action`,
    (arrayFirst(attr -> (attr.1 = 'owner'), `event_attributes_parsed`)).2 AS `attr_owner`,
    (arrayFirst(attr -> (attr.1 = 'token_0_denom'), `event_attributes_parsed`)).2 AS `attr_token_0_denom`,
    (arrayFirst(attr -> (attr.1 = 'token_1_denom'), `event_attributes_parsed`)).2 AS `attr_token_1_denom`,
    (arrayFirst(attr -> (attr.1 = 'token_0_symbol'), `event_attributes_parsed`)).2 AS `attr_token_0_symbol`,
    (arrayFirst(attr -> (attr.1 = 'token_1_symbol'), `event_attributes_parsed`)).2 AS `attr_token_1_symbol`,
    (arrayFirst(attr -> (attr.1 = 'token_0_quote_currency'), `event_attributes_parsed`)).2 AS `attr_token_0_quote_currency`,
    (arrayFirst(attr -> (attr.1 = 'token_1_quote_currency'), `event_attributes_parsed`)).2 AS `attr_token_1_quote_currency`,
    (arrayFirst(attr -> (attr.1 = 'token_0_exponent'), `event_attributes_parsed`)).2 AS `attr_token_0_exponent`,
    (arrayFirst(attr -> (attr.1 = 'token_1_exponent'), `event_attributes_parsed`)).2 AS `attr_token_1_exponent`,
    (arrayFirst(attr -> (attr.1 = 'token_0_decimals'), `event_attributes_parsed`)).2 AS `attr_token_0_decimals`,
    (arrayFirst(attr -> (attr.1 = 'token_1_decimals'), `event_attributes_parsed`)).2 AS `attr_token_1_decimals`,
    (arrayFirst(attr -> (attr.1 = 'max_blocks_stale_token_a'), `event_attributes_parsed`)).2 AS `attr_max_blocks_stale_token_a`,
    (arrayFirst(attr -> (attr.1 = 'max_blocks_stale_token_b'), `event_attributes_parsed`)).2 AS `attr_max_blocks_stale_token_b`,
    (arrayFirst(attr -> (attr.1 = 'max_blocks_stale_token_0'), `event_attributes_parsed`)).2 AS `attr_max_blocks_stale_token_0`,
    (arrayFirst(attr -> (attr.1 = 'max_blocks_stale_token_1'), `event_attributes_parsed`)).2 AS `attr_max_blocks_stale_token_1`,
    (arrayFirst(attr -> (attr.1 = 'pool_id'), `event_attributes_parsed`)).2 AS `attr_pool_id`,
    (arrayFirst(attr -> (attr.1 = 'deposit_cap'), `event_attributes_parsed`)).2 AS `attr_deposit_cap`,
    (arrayFirst(attr -> (attr.1 = 'timestamp_stale'), `event_attributes_parsed`)).2 AS `attr_timestamp_stale`,
    (arrayFirst(attr -> (attr.1 = 'fee_tier_config'), `event_attributes_parsed`)).2 AS `attr_fee_tier_config`,
    (arrayFirst(attr -> (attr.1 = 'paused'), `event_attributes_parsed`)).2 AS `attr_paused`,
    (arrayFirst(attr -> (attr.1 = 'skew'), `event_attributes_parsed`)).2 AS `attr_skew`,
    (arrayFirst(attr -> (attr.1 = 'imbalance'), `event_attributes_parsed`)).2 AS `attr_imbalance`,
    (arrayFirst(attr -> (attr.1 = 'oracle_contract'), `event_attributes_parsed`)).2 AS `attr_oracle_contract`,
    (arrayFirst(attr -> (attr.1 = 'oracle_price_skew'), `event_attributes_parsed`)).2 AS `attr_oracle_price_skew`,
    (arrayFirst(attr -> (attr.1 = 'denom'), `event_attributes_parsed`)).2 AS `attr_denom`,
    extractAll(`attr_owner`, '(?:Addr\(\"(\w+)\"\))') as `attr_owner_array`,
    arrayMap(
        (match) -> (toUInt64OrZero(match[1]), toUInt64OrZero(match[2])),
        extractAllGroupsVertical(`attr_fee_tier_config`, '(?: fee: (\d+), percentage: (\d+))')
    ) as `attr_fee_tier_array`
SELECT
    `timestamp`,
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`,
    -- add event attributes
    `attributes`,
    `contract_address`,
    `action`,
    if (empty(`attr_owner_array`), NULL, concat('["', arrayStringConcat(`attr_owner_array`, '", "'), '"]')) as `whitelist`,
    if (empty(`attr_token_0_denom`), NULL, `attr_token_0_denom`) as `token_0_denom`,
    if (empty(`attr_token_1_denom`), NULL, `attr_token_1_denom`) as `token_1_denom`,
    if (empty(`attr_token_0_symbol`), NULL, `attr_token_0_symbol`) as `token_0_symbol`,
    if (empty(`attr_token_1_symbol`), NULL, `attr_token_1_symbol`) as `token_1_symbol`,
    if (empty(`attr_token_0_quote_currency`), NULL, `attr_token_0_quote_currency`) as `token_0_quote_currency`,
    if (empty(`attr_token_1_quote_currency`), NULL, `attr_token_1_quote_currency`) as `token_1_quote_currency`,
    COALESCE(
        if (
            empty(`attr_token_0_decimals`),
            toUInt8OrNull(`attr_token_0_exponent`),
            toUInt8OrNull(`attr_token_0_decimals`)
        ),
        if (
            empty(`attr_token_0_symbol`),
            NULL,
            if (
                "attr_token_0_symbol" in ('ETH', 'DYDX'),
                18,
                if (
                    "attr_token_0_symbol" = 'BTC',
                    8,
                    6
                )
            )
        )
    ) as `token_0_decimals`,
    COALESCE(
        if (
            empty(`attr_token_1_decimals`),
            toUInt8OrNull(`attr_token_1_exponent`),
            toUInt8OrNull(`attr_token_1_decimals`)
        ),
        if (
            empty(`attr_token_1_symbol`),
            NULL,
            if (
                "attr_token_1_symbol" in ('ETH', 'DYDX'),
                18,
                if (
                    "attr_token_1_symbol" = 'BTC',
                    8,
                    6
                )
            )
        )
    ) as `token_1_decimals`,
    if (
        empty(`attr_max_blocks_stale_token_0`),
        toUInt64OrNull(`attr_max_blocks_stale_token_a`),
        toUInt64OrNull(`attr_max_blocks_stale_token_0`)
    ) as `token_0_max_blocks_old`,
    if (
        empty(`attr_max_blocks_stale_token_1`),
        toUInt64OrNull(`attr_max_blocks_stale_token_b`),
        toUInt64OrNull(`attr_max_blocks_stale_token_1`)
    ) as `token_1_max_blocks_old`,
    if (empty(`attr_pool_id`), NULL, `attr_pool_id`) as `pool_id`,
    toUInt128OrNull(`attr_deposit_cap`) as `deposit_cap`,
    toUInt64OrNull(`attr_timestamp_stale`) as `timestamp_stale`,
    if (
        empty(`attr_fee_tier_array`),
        NULL,
        concat(
            '[',
                arrayStringConcat(
                    arrayMap(
                        (tuple) -> concat('{ "fee": ', tuple.1, ', "percentage": ', tuple.2 , ' }'),
                        `attr_fee_tier_array`
                    ),
                    ','
                ),
            ']'
        )
    ) as `fee_tier_config`,
    if (`attr_paused` = 'true', true, if(`attr_paused` = 'false', false, NULL)) as `paused`, -- note: toBool() may throw an exception
    if (`attr_skew` = 'true', true, if(`attr_skew` = 'false', false, NULL)) as `skew`, -- note: toBool() may throw an exception
    toUInt32OrNull(`attr_imbalance`) as `imbalance`,
    if (empty(`attr_oracle_contract`), NULL, `attr_oracle_contract`) as `oracle_contract`,
    toInt32OrNull(`attr_oracle_price_skew`) as `oracle_price_skew`,
    if (empty(`attr_denom`), NULL, `attr_denom`) as `denom`
FROM spacebox.parsed_event
ARRAY JOIN (
    -- Extract "message part" events with event_index
    arrayFlatten(
        arrayMap(
            (msg_event_parsed, msg_event_index) -> arrayMap(
                (msg_event_attributes_parsed) -> (
                    -- event_tuple.1: event_index
                    toInt32(`msg_events_index_offset` + msg_event_index - 1),
                    -- event_tuple.2: msg_event_attributes_parsed
                    msg_event_attributes_parsed,
                    -- event_tuple.3: event_attributes (string)
                    toJSONString(
                        arrayMap(
                            (attr) -> map('key', (attr).1, 'value', (attr).2, 'index', toString((attr).3)),
                            msg_event_parsed.2
                        )
                    )
                ),
                -- filter to only successful execution events
                arrayFilter(
                    (msg_event_attributes_parsed) -> (
                        -- is action=("instantiate IMM" OR "update_config" OR "create_token")
                        (
                            arrayFirst(
                                (attr) -> attr.1 = 'action',
                                msg_event_attributes_parsed
                            )
                        ).2 in (
                            'instantiate IMM',
                            'update_config',
                            'create_token'
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
            -- enumerate each (msg_event, msg_event_index) within a message part
            `msg_events_parsed`,
            arrayEnumerate(`msg_events_parsed`)
        )
    )
) AS `event_tuple`
WHERE (
    -- test for instinstantiate or update with at least one property
     `action` in ('instantiate IMM', 'update_config') AND (
        `whitelist` IS NOT NULL OR
        `token_0_denom` IS NOT NULL OR
        `token_1_denom` IS NOT NULL OR
        `token_0_symbol` IS NOT NULL OR
        `token_1_symbol` IS NOT NULL OR
        `token_0_quote_currency` IS NOT NULL OR
        `token_1_quote_currency` IS NOT NULL OR
        `token_0_decimals` IS NOT NULL OR
        `token_1_decimals` IS NOT NULL OR
        `token_0_max_blocks_old` IS NOT NULL OR
        `token_1_max_blocks_old` IS NOT NULL OR
        `pool_id` IS NOT NULL OR
        `deposit_cap` IS NOT NULL OR
        `oracle_contract` IS NOT NULL OR
        `imbalance` IS NOT NULL OR
        `skew` IS NOT NULL OR
        `timestamp_stale` IS NOT NULL OR
        `paused` IS NOT NULL OR
        `fee_tier_config` IS NOT NULL
     )
) OR (
    -- test for create_token with denom
    `action` = 'create_token' AND
    notEmpty(`denom`)
);

-- TODO: also parse contract migration messages
-- like: https://www.mintscan.io/neutron/tx/1A5DE988F3B2A34D701AED1AB5DB4C36C29F0F30D0DA99051A4DF83C55EA6BB2?sector=message
