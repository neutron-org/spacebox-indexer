# How to sync tables between Spacebox indexers

## Step 1 setup an SSH tunnel for Spacebox remote TCP requests

eg. read "Braxion" data from "Zalthos"

```shell
ssh delta-zalthos.neutron.org
# read braxion from zalthos
# need TCP port not HTTP port
ssh -i ~/.ssh/zalthos_remote_ch_queries -N -L 0.0.0.0:19000:127.0.0.1:9000 neutron@195.201.85.164

# check that a remote request works from inside the receiving instance Docker container
USER=api
PASS=...
docker exec -it $( docker ps -q --filter name=spacebox-clickhouse-1 ) clickhouse-client --query "
    SELECT height FROM remote('host.docker.internal:19000', 'spacebox', 'raw_block_results', '$USER', '$PASS') LIMIT 1
"

# go into the Docker instance to set up a script for copying table data
docker exec -it $( docker ps -q --filter name=spacebox-clickhouse-1 ) bash

USER=api
PASS=...
ROWS=10

cd /home
```

## remote_sync.sh

eg. `bash remote_sync.sh "$USER" "$PASS" 100`

```shell
#!/usr/bin/env bash
set -euo pipefail

USERNAME="$1"
PASSWORD="$2"
ROWCOUNT="${3:-10}"

# re-run any previously aborted query
if [[ -s "/home/remote_sync_rows.csv"  ]];
then

    echo "start $(date -u +"%Y-%m-%dT%H:%M:%SZ"): $( cat /home/remote_sync_rows.csv )" >> /home/remote_sync_log.txt

    clickhouse-client --time --query "
        INSERT INTO spacebox.raw_block_results
        SELECT *
        FROM remote('host.docker.internal:19000', 'spacebox', 'raw_block_results', '$USERNAME', '$PASSWORD')
        WHERE height IN ($( cat /home/remote_sync_rows.csv ))
        ORDER BY height ASC
        SETTINGS
            optimize_read_in_order = 1,
            max_bytes_before_external_group_by = 1e9, -- 1 GiB
            max_bytes_before_external_sort = 1e9, -- 1 GiB
            memory_usage_overcommit_max_wait_microseconds = 10000000; -- 10 seconds
    "

    echo "stop  $(date -u +"%Y-%m-%dT%H:%M:%SZ"): $( cat /home/remote_sync_rows.csv )" >> /home/remote_sync_log.txt
else
  # add starting condition
  echo "init" > /home/remote_sync_rows.csv
fi

# start loop until done
# note: could run always in background on loop if given no exit condition
while [[ -s "/home/remote_sync_rows.csv" ]]; do

    # find a minimum height to start counting missing heights from
    LAST_CONSECUTIVE_ROW="$(
        clickhouse-client --time --query "
            WITH numbered AS (
                SELECT
                    height,
                    row_number() OVER (ORDER BY height) AS rn,
                    height - rn AS grp
                FROM spacebox.raw_block_results
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
        WITH target_table as (SELECT height FROM spacebox.raw_block_results),
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
    " > /home/remote_sync_rows.csv

    echo "start $(date -u +"%Y-%m-%dT%H:%M:%SZ"): $( cat /home/remote_sync_rows.csv )" >> /home/remote_sync_log.txt

    clickhouse-client --time --query "
        INSERT INTO spacebox.raw_block_results
        SELECT *
        FROM remote('host.docker.internal:19000', 'spacebox', 'raw_block_results', '$USERNAME', '$PASSWORD')
        WHERE height IN ($( cat /home/remote_sync_rows.csv ))
        ORDER BY height ASC
        SETTINGS
            optimize_read_in_order = 1,
            max_bytes_before_external_group_by = 1e9, -- 1 GiB
            max_bytes_before_external_sort = 1e9, -- 1 GiB
            memory_usage_overcommit_max_wait_microseconds = 10000000; -- 10 seconds
    "

    echo "stop  $(date -u +"%Y-%m-%dT%H:%M:%SZ"): $( cat /home/remote_sync_rows.csv )" >> /home/remote_sync_log.txt

    # pause
    sleep 1
done

echo "synced"
```

## Step 3: run the script and observe the logs

```shell
# run script
bash /home/remote_sync.sh "$USER" "$PASS" 100

# observe logs
tail /home/remote_sync_log.txt;
```
