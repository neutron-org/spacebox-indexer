
# Getting started

## Get Dependencies

### Get Crawler

```shell
cd ~
git clone https://github.com/neutron-org/spacebox-crawler.git spacebox-crawler
cd spacebox-crawler
git checkout fix/reprocess-blocks
sudo docker build -t spacebox-crawler:reprocess-blocks .
```

### Get Indexer

```shell
cd ~
git clone https://github.com/neutron-org/spacebox-indexer.git spacebox
cd spacebox
git checkout feat/update-neutron-dex-events-and-state-tables
# edit any vars
vi .env
vi docker-compose.yaml
# add custom config files if needed
cd ~/spacebox/config/clickhouse/config.d
# add custom user config files if needed
cd ~/spacebox/config/clickhouse/users.d
```

### Maybe get old Docker images from previous instances

Copy old Bitnami Docker images from old servers if needed:

```shell
# @source instance
sudo docker save bitnami/kafka:3.7.0 | gzip > /tmp/kafka.tar.gz
sudo docker save bitnami/zookeeper:3.9.2 | gzip > /tmp/zookeeper.tar.gz

# @localhost receive and send images
scp neutron@delta-braxion.neutron.org:/tmp/kafka.tar.gz .
scp neutron@delta-braxion.neutron.org:/tmp/zookeeper.tar.gz .

scp kafka.tar.gz neutron@delta-braxion-2.neutron.org:/tmp
scp zookeeper.tar.gz neutron@delta-braxion-2.neutron.org:/tmp

# @destination instance
gunzip -c /tmp/kafka.tar.gz | sudo docker load
gunzip -c /tmp/zookeeper.tar.gz | sudo docker load
```

## Start the services

```shell
# start services (copy old Bitnami Docker images from old servers if needed)
cd ~/spacebox

sh setup.sh
sudo docker compose up -d
```

## Managing the service

### Kafka

```shell
# see queues
sudo docker exec -it $(sudo docker ps -q --filter name=spacebox-kafka-1 ) /opt/bitnami/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
# see queue lag
sudo docker exec -it $(sudo docker ps -q --filter name=spacebox-kafka-1 ) /opt/bitnami/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group spacebox --offsets
```

### Clickhouse Client

```shell
# enter the Clickhouse client in the Clickhouse container
sudo docker exec -it $(sudo docker ps -q --filter name=spacebox-clickhouse-1 ) clickhouse-client
# or do a single query directly
sudo docker exec -it $(sudo docker ps -q --filter name=spacebox-clickhouse-1 ) clickhouse-client --query "SELECT version()"
```

#### Upgrading

After updating the `docker-compose.yaml` file

```shell
# eg. updating clickhouse version in docker-compose
docker compose up -d --force-recreate --no-deps clickhouse
```

#### Helpful SQL queries

Get size of tables

```sql
    SELECT
        database,
        name AS table,
        formatReadableSize(total_bytes) AS bytes_on_disk
    FROM system.tables
    WHERE total_bytes IS NOT NULL
    ORDER BY database ASC, total_bytes DESC;
```

Trimming irrelevant data to save space

```sql
-- parsed_event is an intermediary table it does not need to always persist data
ALTER TABLE spacebox.parsed_event MODIFY TTL timestamp + toIntervalDay(30);
-- some system tables contain just logs and can get very large
ALTER TABLE system.text_log MODIFY TTL event_time + toIntervalDay(7);
ALTER TABLE system.query_views_log MODIFY TTL event_time + toIntervalDay(7);
-- to apply a TTL immediately
ALTER TABLE system.text_log MATERIALIZE TTL;
```

See Kafka tables

```sql
SELECT * FROM system.tables WHERE engine = 'Kafka';
```

See materialized views

```sql
    SELECT
        database,
        name AS view
    FROM system.tables
    WHERE engine = 'MaterializedView'
    ORDER BY database, view ASC;
```

See refreshable materialized views

```sql
    SELECT * FROM system.view_refreshes;
    -- RUN one of these views immediately
    SYSTEM REFRESH VIEW spacebox.dex_swaps_valued_daily_writer;
    SYSTEM REFRESH VIEW spacebox.dex_vaults_shares_valued_daily_writer;
    SYSTEM REFRESH VIEW spacebox.dex_vaults_dex_balance_valued_daily_writer;
```

#### Checking data validity

##### Find unparsed DEX row data

There are very few rows without DEX data since the Supervaults started running,
these can be queried here:

```sql
WITH target_table as (SELECT DISTINCT height FROM spacebox.parsed_dex_message_event_action),
height_sequence as (
    -- note: Supervaults first started at height 28887908
    SELECT arrayJoin(range(28887908, max(height) - 100)) AS height
    FROM target_table
),
missing_heights as (
    SELECT height
    FROM height_sequence
    LEFT JOIN target_table ON height = target_table.height
    WHERE target_table.height = 0 OR target_table.height IS NULL
)
SELECT height FROM missing_heights;
```

The result should be about the following known heights without DEX events:

```text
    ┌───height─┐
 1. │ 36430004 │
 2. │ 36430005 │
 3. │ 36430006 │
 4. │ 36430007 │
 5. │ 36430008 │
 6. │ 36430009 │
 7. │ 46890955 │
 8. │ 46890956 │
 9. │ 46891066 │
10. │ 46937998 │
11. │ 46938071 │
12. │ 46980535 │
13. │ 46980592 │
14. │ 46980624 │
15. │ 46980625 │
16. │ 46980896 │
17. │ 47266466 │
    └──────────┘
```

if more results than this are found then run a backfill query:

```sql
WITH target_table as (SELECT DISTINCT height FROM spacebox.parsed_dex_message_event_action),
height_sequence as (
    -- note: Supervaults first started at height 28887908
    SELECT arrayJoin(range(28887908, max(height) - 100)) AS height
    FROM target_table
),
missing_heights as (
    SELECT height
    FROM height_sequence
    LEFT JOIN target_table ON height = target_table.height
    WHERE target_table.height = 0 OR target_table.height IS NULL
)
INSERT INTO spacebox.raw_block_results_backfill
SELECT * FROM spacebox.raw_block_results
WHERE height IN (SELECT height FROM missing_heights)
```

#### Debugging queries

##### Reattaching broken Kafka tables

if a Kafka service appears to be stopped (a table appears to not be connected as a consumer)

```shell
# if there appears to be no consumer ID for a Kafka table in these queries
sudo docker exec -it $(sudo docker ps -q --filter name=spacebox-kafka-1 ) /opt/bitnami/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group spacebox --offsets
sudo docker exec -it $(sudo docker ps -q --filter name=spacebox-clickhouse-1 ) clickhouse-client --query "SELECT * FROM system.tables WHERE engine = 'Kafka'"
```

Then you may have to re-attach the Kafka table to start it consuming again
see: https://clickhouse.com/docs/integrations/kafka/kafka-table-engine#common-operations

```sql
DETACH TABLE spacebox.raw_block_results_topic;
ATTACH TABLE spacebox.raw_block_results_topic;
```

##### How to recover from an app hash issue

```sql
-- for removing an example bad AppHash data of block 37545431
ALTER TABLE spacebox.bank_transfer DELETE WHERE height = 37545431;
ALTER TABLE spacebox.bank_transfer_by_address_then_denom DELETE WHERE height = 37545431;
ALTER TABLE spacebox.dex_message_event_deposit_lp DELETE WHERE height = 37545431;
ALTER TABLE spacebox.dex_message_event_tick_state DELETE WHERE height = 37545431;
ALTER TABLE spacebox.dex_message_event_tick_update DELETE WHERE height = 37545431;
ALTER TABLE spacebox.dex_message_event_tranche_user_update DELETE WHERE height = 37545431;
ALTER TABLE spacebox.dex_message_event_withdraw_lp DELETE WHERE height = 37545431;
ALTER TABLE spacebox.dex_swaps DELETE WHERE height = 37545431;
ALTER TABLE spacebox.dex_swaps_valued DELETE WHERE height = 37545431;
ALTER TABLE spacebox.dex_vaults_config_event DELETE WHERE height = 37545431;
ALTER TABLE spacebox.dex_vaults_dex_balance DELETE WHERE height = 37545431;
ALTER TABLE spacebox.dex_vaults_dex_balance_valued DELETE WHERE height = 37545431;
ALTER TABLE spacebox.dex_vaults_dex_balance_valued_by_minute_agg DELETE WHERE height = 37545431;
ALTER TABLE spacebox.dex_vaults_events_dex_deposit DELETE WHERE height = 37545431;
ALTER TABLE spacebox.dex_vaults_shares DELETE WHERE height = 37545431;
ALTER TABLE spacebox.dex_vaults_shares_valued DELETE WHERE height = 37545431;
ALTER TABLE spacebox.parsed_event DELETE WHERE height = 37545431;
ALTER TABLE spacebox.parsed_dex_message_event DELETE WHERE height = 37545431;
ALTER TABLE spacebox.parsed_dex_message_event_action DELETE WHERE height = 37545431;
ALTER TABLE spacebox.price_by_vault_denom DELETE WHERE height = 37545431;
ALTER TABLE spacebox.price_by_vault_denom_by_minute_agg DELETE WHERE height = 37545431;
ALTER TABLE spacebox.raw_block_results DELETE WHERE height = 37545431;
```

A remote Clickhouse server query

```sql
    SELECT *
    FROM remote(
        'host.docker.internal:19000',
        'spacebox',
        'dex_vaults_shares',
        '${user: String}',
        '${password: String}'
    )
    LIMIT 1
```
