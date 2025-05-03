
CREATE TABLE spacebox.fixes
(
    `id`                UInt16,
    `description`       String,
    -- note: fixes are only applied when all conditions are met
    --       hopefully they are only run once, but they should be able to be run multiple times in the case of race conditions
    `applied`           Boolean DEFAULT 0,
    `height`            Int64 DEFAULT 0,
    -- add versioning to default to earliest matching block
    `version`           Float64 DEFAULT if(`height`>0, 1/`height`, 0)
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree()
ORDER BY `id`
SETTINGS index_granularity = 8192;

-- can add known fixes here
INSERT INTO spacebox.fixes (`id`, `description`) VALUES
(1, 'swap-volume: TickUpdate event SwapAmountIn/SwapAmountOut attributes'),
(2, 'dex pool-id: dex action=DepositLP/WithdrawLP event PoolId attribute')

-- spacebox.fix_1_writer source

CREATE MATERIALIZED VIEW spacebox.fix_1_trigger TO spacebox.fixes (
    `id`                UInt16,
    `description`       String,
    `applied`           Boolean,
    `height`            Int64
) AS
    WITH
    fix AS (
        SELECT `applied`
        FROM spacebox.fixes
        WHERE `id` = 1
          AND `applied` = 0
    ),
    missing_block_check AS (
        SELECT max(`height`) - min(`height`) - count(*) + 1 AS `missing_block_count`
        FROM spacebox.raw_block_results
    )
    SELECT
        1 as `id`,
        'swap-volume: TickUpdate event SwapAmountIn/SwapAmountOut attributes' as `description`,
        1 as `applied`,
        `height`
    FROM spacebox.message_event
        -- only add if fix is not yet applied
        INNER JOIN fix ON 1=1
        INNER JOIN missing_block_check ON 1=1
    WHERE
        `missing_block_count` = 0 AND
        arrayExists(
            (msg_event) -> (
                JSONExtractString(msg_event, 'type') = 'TickUpdate' AND
                arrayExists(
                    (attr) -> JSONExtractString(attr, 'key') = 'SwapAmountIn',
                    JSONExtractArrayRaw(msg_event, 'attributes')
                )
            ),
            `msg_events`
        )
    ORDER BY `height` ASC
    LIMIT 1;

CREATE MATERIALIZED VIEW spacebox.fix_1_applier TO spacebox.dex_message_event_tick_update (
    `timestamp`         DateTime,
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `type`              LowCardinality(String),
    `action`            LowCardinality(String),
    `TokenZero`         LowCardinality(String),
    `TokenOne`          LowCardinality(String),
    `TokenIn`           LowCardinality(String),
    `TickIndex`         Int64,
    `Fee`               UInt64,
    `TrancheKey`        String,
    `Reserves`          UInt256,
    -- added after DEX v5 (see https://github.com/neutron-org/neutron/pull/808)
    `SwapAmountIn`      UInt256,
    `SwapAmountOut`     UInt256,
    -- added to calculate SwapAmountIn/Out for DEX v<=5 events
    `is_swap`           Boolean,
    `is_estimated_swap` Boolean,
    `version`           UInt8
) AS
    WITH
    -- only add if fix is applied
    fix AS (
        SELECT `applied`
        FROM spacebox.fixes
        WHERE `id` = 1
          AND `applied` = 1
    ),
    -- get entire swap volume fix
    swap_volume_fix AS (
        -- get previous reserves value by using an ordered window to select previous (by order) row data
        -- to help determine the ReservesDelta field: the current - previous Reserves value
        WITH lagInFrame(`Reserves`, 1, 0) OVER (
            -- partition by "pools" of reserves (they are separate per tick + fee/tranche combination)
            PARTITION BY `TokenZero`, `TokenOne`, `TokenIn`, `TickIndex`, `Fee`, `TrancheKey`
            -- within the pool index partition, sort by event order
            ORDER BY `height` ASC, `block_part_index` ASC, `tx_index` ASC, `event_index` ASC
        ) as `PreviousReserves`,
        -- compare this to current row data to get relative state (ReservesDelta)
        (t.`Reserves` - `PreviousReserves`) as `ReservesDelta`
        -- use the already derived is_estimated_swap field to compute new SwapAmountIn and SwapAmountOut attributes
        SELECT
            -- pass all fields
            t.*,
            t.`is_swap` AND t.`SwapAmountOut` = 0 as `is_estimated_swap`,
            -- attach fixed computed fields
            if (
                -- note: all swap TickUpdate events should be DEX decrements (ReservesDelta < 0)
                `is_estimated_swap` AND `ReservesDelta` < 0,
                toUInt128(abs(`ReservesDelta`)),
                0
            ) as `SwapAmountOut`,
            if (
                -- note: SwapAmountIn may have rounding errors (but this very small in practice)
                `is_estimated_swap` AND `ReservesDelta` < 0,
                toUInt128(ceiling(multiply(toFloat64(abs(`ReservesDelta`)), pow(1.0001, `TickIndex`)))),
                0
            ) as `SwapAmountIn`
        FROM spacebox.dex_message_event_tick_update as t
    )
    SELECT
        `timestamp`,
        `height`,
        `block_part_index`,
        `tx_index`,
        `event_index`,
        `type`,
        `action`,
        `TokenZero`,
        `TokenOne`,
        `TokenIn`,
        `TickIndex`,
        `Fee`,
        `TrancheKey`,
        `Reserves`,
        `SwapAmountIn`,
        `SwapAmountOut`,
        `SwapAmountOut` > 0 as `is_swap`,
        `is_estimated_swap`,
        -- ensure this overwrites the first version on data from the original MV
        2 as `version`
    FROM fix
        LEFT JOIN swap_volume_fix ON 1=1
    WHERE `is_swap` = 1
      AND `is_estimated_swap` = 1;
SETTINGS
    -- do not wait for acknowledgement of insert (it should handle race conditions fine):
    -- on testnet this fix took 20s to apply
    async_insert = 1,
    wait_for_async_insert = 0;

-- spacebox.fix_2_writer source

CREATE MATERIALIZED VIEW spacebox.fix_2_trigger TO spacebox.fixes (
    `id`                UInt16,
    `description`       String,
    `applied`           Boolean,
    `height`            Int64
) AS
    SELECT
        2 as `id`,
        'dex pool-id: dex action=DepositLP/WithdrawLP event PoolID attribute' as `description`,
        1 as `applied`,
        `height`
    FROM spacebox.message_event
        -- only add if fix is not yet applied
        INNER JOIN (
            SELECT `applied`
            FROM spacebox.fixes
            WHERE `id` = 2
              AND `applied` = 0
        ) as fix ON 1=1
    WHERE arrayExists(
        (msg_event) -> (
            JSONExtractString(msg_event, 'type') = 'message' AND
            -- is a dex event
            arrayExists(
                (attr) -> (
                    JSONExtractString(attr, 'key') = 'module' AND
                    JSONExtractString(attr, 'value') = 'dex'
                ),
                JSONExtractArrayRaw(msg_event, 'attributes')
            ) AND
            -- is a deposit or withdrawal
            arrayExists(
                (attr) -> (
                    JSONExtractString(attr, 'key') = 'action' AND
                    JSONExtractString(attr, 'value') IN ('DepositLP', 'WithdrawLP')
                ),
                JSONExtractArrayRaw(msg_event, 'attributes')
            ) AND
            -- has a PoolID attribute
            arrayExists(
                (attr) -> JSONExtractString(attr, 'key') = 'PoolID',
                JSONExtractArrayRaw(msg_event, 'attributes')
            )
        ),
        `msg_events`
    )
    ORDER BY height ASC
    LIMIT 1;
