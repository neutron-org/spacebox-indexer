
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
(2, 'dex pool-id: dex action=DepositLP/WithdrawLP event PoolId attribute');

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
