#!/usr/bin/env bash
set -euo pipefail

# usage eg. bash /home/remote_sync.sh "$USER" "$PASS" raw_block_results 100

USERNAME="$1"
PASSWORD="$2"
TABLE="$3"
ROWCOUNT="${4:-10}"
DELAY="${5:-1}"

mkdir -p "/home/scripts/logs"
PREFIX="/home/scripts/logs/$TABLE"

# re-run any previously aborted query
if [[ -s "${PREFIX}_remote_syncing_rows.csv" ]];
then

    echo "start $(date -u +"%Y-%m-%dT%H:%M:%SZ"): $( cat "${PREFIX}_remote_syncing_rows.csv" )" >> "${PREFIX}_remote_sync_log.txt"

    clickhouse-client --time --query "
        INSERT INTO spacebox.$TABLE
        SELECT *
        FROM remote('host.docker.internal:19000', 'spacebox', '$TABLE', '$USERNAME', '$PASSWORD')
        WHERE height IN ($( cat "${PREFIX}_remote_syncing_rows.csv" ))
        ORDER BY height ASC
        SETTINGS
            optimize_read_in_order = 1,
            max_bytes_before_external_group_by = 1e9, -- 1 GiB
            max_bytes_before_external_sort = 1e9, -- 1 GiB
            memory_usage_overcommit_max_wait_microseconds = 10000000; -- 10 seconds
    "

    echo "stop  $(date -u +"%Y-%m-%dT%H:%M:%SZ"): $( cat "${PREFIX}_remote_syncing_rows.csv" )" >> "${PREFIX}_remote_sync_log.txt"
else
  # add starting condition
  echo "init" > "${PREFIX}_remote_syncing_rows.csv"
fi

# start loop until done
# note: could run always in background on loop if given no exit condition
while [[ -s "${PREFIX}_remote_syncing_rows.csv" ]]; do

    # find a minimum height to start counting missing heights from
    LAST_CONSECUTIVE_ROW="$(
        clickhouse-client --time --query "
            WITH numbered AS (
                SELECT
                    height,
                    dense_rank() OVER (ORDER BY height) AS rn,
                    height - rn AS grp
                FROM spacebox.$TABLE
                WHERE height >= ${LAST_CONSECUTIVE_ROW:-0}
            ),
            first_grp AS (SELECT min(grp) AS g FROM numbered)
            SELECT
                max(height) AS last_consecutive
            FROM numbered
            WHERE grp = (SELECT g FROM first_grp);
        "
    )"

    echo "syncing... LAST_CONSECUTIVE_ROW: $LAST_CONSECUTIVE_ROW"

    clickhouse-client --format Values --query "
        WITH target_table as (SELECT height FROM spacebox.$TABLE),
        height_sequence as (
            SELECT arrayJoin(range(greatest(min(height),$LAST_CONSECUTIVE_ROW), max(height) + 1)) AS height
            FROM target_table
        )
        SELECT height
        FROM height_sequence
        LEFT JOIN target_table ON height_sequence.height = target_table.height
        WHERE target_table.height IS NULL OR target_table.height = 0
        ORDER BY height asc
        LIMIT $ROWCOUNT
    " > "${PREFIX}_remote_syncing_rows.csv"

    echo "start $(date -u +"%Y-%m-%dT%H:%M:%SZ"): $( cat "${PREFIX}_remote_syncing_rows.csv" )" >> "${PREFIX}_remote_sync_log.txt"

    clickhouse-client --time --query "
        INSERT INTO spacebox.$TABLE
        SELECT *
        FROM remote('host.docker.internal:19000', 'spacebox', '$TABLE', '$USERNAME', '$PASSWORD')
        WHERE height IN ($( cat "${PREFIX}_remote_syncing_rows.csv" ))
        ORDER BY height ASC
        SETTINGS
            optimize_read_in_order = 1,
            max_bytes_before_external_group_by = 1e9, -- 1 GiB
            max_bytes_before_external_sort = 1e9, -- 1 GiB
            memory_usage_overcommit_max_wait_microseconds = 10000000; -- 10 seconds
    "

    echo "stop  $(date -u +"%Y-%m-%dT%H:%M:%SZ"): $( cat "${PREFIX}_remote_syncing_rows.csv" )" >> "${PREFIX}_remote_sync_log.txt"

    # pause
    sleep "$DELAY"
done

echo "synced"