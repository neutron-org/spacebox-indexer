# How to sync tables between Spacebox indexers

## Step 1: setup an SSH tunnel for Spacebox remote TCP requests

eg. read "Braxion" data from "Zalthos"

```shell
ssh delta-zalthos.neutron.org
# read braxion from zalthos
# need TCP port (9000) not HTTP port, expose as local port 19000 to avoid clash
ssh -i ~/.ssh/zalthos_remote_ch_queries -N -L 0.0.0.0:19000:127.0.0.1:9000 neutron@195.201.85.164

# check that a remote request works from inside the receiving instance Docker container
USER=api
PASS=...
docker exec -it $( docker ps -q --filter name=spacebox-clickhouse-1 ) clickhouse-client --query "
    SELECT height FROM remote('host.docker.internal:19000', 'spacebox', 'raw_block_results', '$USER', '$PASS') LIMIT 1
"
```

## Step 2: go into the running Clickhouse container to use the syncing script

```shell

# go into the Docker instance to use the script for copying table data
docker exec -it $( docker ps -q --filter name=spacebox-clickhouse-1 ) bash

USER=api
PASS=...

# use the syncing script
cd /home/scripts
bash remote_sync.sh "$USER" "$PASS" raw_block
bash remote_sync.sh "$USER" "$PASS" raw_block_results
bash remote_sync.sh "$USER" "$PASS" raw_transaction # not tested, may not work

# observe logs
tail /home/scripts/logs/raw_block_remote_sync_log.txt;
tail /home/scripts/logs/raw_block_results_remote_sync_log.txt;
tail /home/scripts/logs/raw_transaction_remote_sync_log.txt;
```
