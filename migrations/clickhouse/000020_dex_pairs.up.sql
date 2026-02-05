
CREATE TABLE spacebox.dex_pairs_agg
(
    `TokenZero`         LowCardinality(String),
    `TokenOne`          LowCardinality(String),
    `created_at_height` AggregateFunction(min, Int64),
    `updated_at_height` AggregateFunction(max, Int64),
    `created_at`        AggregateFunction(min, DateTime64(9)),
    `updated_at`        AggregateFunction(max, DateTime64(9)),
    `inserted_at`       DateTime64(9) MATERIALIZED nowInBlock64()
)
ENGINE = AggregatingMergeTree()
    ORDER BY (`TokenZero`, `TokenOne`)
SETTINGS index_granularity = 8192;

-- spacebox.dex_pairs_agg_writer source

CREATE MATERIALIZED VIEW IF NOT EXISTS spacebox.dex_pairs_agg_writer TO spacebox.dex_pairs_agg AS
SELECT
    `TokenZero`,
    `TokenOne`,
    minState(t.`height`) as `created_at_height`,
    maxState(t.`height`) as `updated_at_height`,
    minState(t.`timestamp`) as `created_at`,
    maxState(t.`timestamp`) as `updated_at`
FROM spacebox.dex_message_event_tick_update as t
GROUP BY `TokenZero`, `TokenOne`;

CREATE VIEW spacebox.dex_pairs AS
    SELECT
        `TokenZero`,
        `TokenOne`,
        minMerge(`created_at_height`) as `created_at_height`,
        maxMerge(`updated_at_height`) as `updated_at_height`,
        minMerge(`created_at`) as `created_at`,
        maxMerge(`updated_at`) as `updated_at`
    FROM spacebox.dex_pairs_agg
    GROUP BY `TokenZero`, `TokenOne`;
