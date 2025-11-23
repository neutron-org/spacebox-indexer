
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

See materialized views

```sql
    SELECT
        database,
        name AS view
    FROM system.tables
    WHERE engine = 'MaterializedView'
    ORDER BY database, view ASC;
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
