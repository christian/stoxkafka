# Stox Kafka

Small local Kafka setup using Docker, sbt, Scala 3, and the Apache Kafka Java client.

## Start Kafka

```sh
docker compose up -d
```

Kafka listens on `localhost:9092`.

Kafka UI is available at:

```text
http://localhost:8080
```

Use it to inspect topics, partitions, messages, consumer groups, offsets, and lag.

## Start the live app

Run each service in its own terminal.

Terminal 1: Kafka and Kafka UI

```sh
cd /Users/cristi/Code/stox-kafka
docker compose up -d
./scripts/create-trading-topics.sh
```

Terminal 2: 1-minute candle builder

```sh
cd /Users/cristi/Code/stox-kafka
sbt "runMain stoxkafka.OneMinuteCandleService"
```

Terminal 3: WebSocket service

```sh
cd /Users/cristi/Code/stox-kafka
sbt "runMain stoxkafka.CandleWebSocketService"
```

Terminal 4: React dashboard

```sh
cd /Users/cristi/Code/stox-kafka/dashboard
nvm use
npm install
npm run dev
```

Terminal 5: SQLite live store

```sh
cd /Users/cristi/Code/stox-kafka
sbt "runMain stoxkafka.CandleSqliteStorageService"
```

This store keeps the current day in SQLite by default, including the in-progress candle state, so a refresh still shows today's chart.

Terminal 6: IBKR tick producer

```sh
cd /Users/cristi/Code/stox-kafka
uv sync
uv run python/ibkr_tick_producer.py
```

Terminal 7: IBKR options OI snapshot producer

```sh
cd /Users/cristi/Code/stox-kafka
uv sync
uv run python/ibkr_options_oi_producer.py
```

Terminal 8: options OI SQLite consumer

```sh
cd /Users/cristi/Code/stox-kafka
sbt "runMain stoxkafka.OptionsOpenInterestSqliteService"
```

This keeps the raw option-contract snapshots and the daily put/call ratio series in `data/live/options_oi.db`.

Open the live dashboard:

```text
http://localhost:5173
```

Open Kafka UI:

```text
http://localhost:8080
```

The live data path is:

```text
IBKR tick producer
  -> market.trades.raw
  -> OneMinuteCandleService
  -> market.candles.1m
  -> CandleSqliteStorageService
  -> CandleWebSocketService
  -> React dashboard
```

The options OI path is:

```text
IBKR options OI producer
  -> market.options.oi.snapshot
  -> OptionsOpenInterestSqliteService
  -> data/live/options_oi.db
```

Useful health checks:

```sh
curl http://localhost:9000/health
lsof -nP -iTCP:9000 -sTCP:LISTEN
lsof -nP -iTCP:5173 -sTCP:LISTEN
```

## Create a topic

```sh
docker exec stox-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic learning-events \
  --partitions 3 \
  --replication-factor 1
```

## Create trading topics

For the live-candle learning setup:

```sh
chmod +x scripts/create-trading-topics.sh
./scripts/create-trading-topics.sh
```

This creates:

```text
market.trades.raw     24 partitions
market.options.oi.snapshot  6 partitions
market.candles.1s     24 partitions
market.candles.1m     24 partitions
market.candles.5m     12 partitions
market.candles.15m    12 partitions
market.candles.1h      6 partitions
market.candles.1d      3 partitions
```

Suggested keys:

```text
raw trades: exchange:symbol
candles:    exchange:symbol:timeframe
options OI: underlying:expiry:strike:right
```

## Stream IBKR ticks into Kafka

The Python producer in `python/ibkr_tick_producer.py` connects to Trader Workstation or IB Gateway using `ib_async`, subscribes to ASML on Euronext Amsterdam, and writes tick snapshots to `market.trades.raw`.

ASML is the default because it is a highly traded European large-cap with enough realized movement to make a live market-data example interesting. You need the relevant IBKR market data permissions for live data; otherwise use delayed data.

The tick payload also includes a `session` field (`premarket`, `regular`, or `afterhours`) and `isPremarket` boolean so the UI and downstream consumers can keep the extended-hours ticks distinct.

Install the Python dependencies:

```sh
cd /Users/cristi/Code/stox-kafka
uv sync
```

Start Kafka:

```sh
docker compose up -d
./scripts/create-trading-topics.sh
```

Start TWS or IB Gateway, enable API access, then run:

```sh
uv run python/ibkr_tick_producer.py
```

For delayed data:

```sh
uv run python/ibkr_tick_producer.py --market-data-type 3
```

Useful IBKR ports:

```text
TWS paper:      7497
TWS live:       7496
Gateway paper:  4002
Gateway live:   4001
```

Override the instrument:

```sh
uv run python/ibkr_tick_producer.py \
  --symbol SAP \
  --exchange SMART \
  --primary-exchange IBIS \
  --currency EUR
```

## Stream IBKR options open interest into Kafka

The options OI producer snapshots IBKR option contracts for one underlying, reads `callOpenInterest` / `putOpenInterest`, and writes one record per contract to `market.options.oi.snapshot`.

This is a daily snapshot producer, not a live stream. Open interest is updated overnight, so this is the right shape for a scheduled run after IBKR/OCC updates.

Install the Python dependencies:

```sh
cd /Users/cristi/Code/stox-kafka
uv sync
```

Start Kafka:

```sh
docker compose up -d
./scripts/create-trading-topics.sh
```

Start TWS or IB Gateway, enable API access, then run:

```sh
uv run python/ibkr_options_oi_producer.py
```

For delayed data:

```sh
uv run python/ibkr_options_oi_producer.py --market-data-type 3
```

Override the underlying:

```sh
uv run python/ibkr_options_oi_producer.py \
  --symbol SAP \
  --exchange SMART \
  --primary-exchange IBIS \
  --currency EUR \
  --max-expiries 2 \
  --strikes-per-expiry 10
```

The default selection takes the nearest expiries and strikes around the underlying price. Use `--max-expiries 0` and `--strikes-per-expiry 0` for the full chain.

List topics:

```sh
docker exec stox-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

## Run the consumer

In one terminal:

```sh
sbt "runMain stoxkafka.ConsumerApp"
```

## Run the producer

In another terminal:

```sh
sbt "runMain stoxkafka.ProducerApp 10"
```

## Run the 1-minute candle service

The candle service consumes raw IBKR ticks from `market.trades.raw`, builds live 1-minute candles, and produces updates/final candles to `market.candles.1m`.

In one terminal, run the service:

```sh
sbt "runMain stoxkafka.OneMinuteCandleService"
```

In another terminal, watch the generated candles:

```sh
docker exec -it stox-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic market.candles.1m \
  --from-beginning
```

Then run the IBKR tick producer:

```sh
uv run python/ibkr_tick_producer.py
```

The candle key is:

```text
exchange:symbol:1m
```

Example:

```text
ibkr:ASML:1m
```

By default, a candle is finalized 2 seconds after the minute ends. Override that with:

```sh
CANDLE_CLOSE_GRACE_SECONDS=5 sbt "runMain stoxkafka.OneMinuteCandleService"
```

## Store candles as Parquet

The Parquet storage service consumes candles from `market.candles.1m` and writes finalized candles under `data/candles`.

Run it with:

```sh
sbt "runMain stoxkafka.CandleParquetStorageService"
```

Files are partitioned like this:

```text
data/candles/
  exchange=ibkr/
    symbol=ASML/
      timeframe=1m/
        date=2026-05-25/
          part-....parquet
```

By default, only `isFinal=true` candles are stored. To store live candle updates too:

```sh
STORE_FINAL_ONLY=false sbt "runMain stoxkafka.CandleParquetStorageService"
```

Override the output directory:

```sh
CANDLE_PARQUET_DIR=/tmp/stox-candles sbt "runMain stoxkafka.CandleParquetStorageService"
```

## Run the live dashboard

The WebSocket service consumes `market.candles.1m` and broadcasts live candle JSON to browser clients.

Run the Scala WebSocket service:

```sh
sbt "runMain stoxkafka.CandleWebSocketService"
```

Run the React dashboard:

```sh
cd dashboard
nvm use
npm install
npm run dev
```

Open:

```text
http://localhost:5173
```

The dashboard connects to:

```text
ws://localhost:9000/ws/candles?symbol=ASML&timeframe=1m
```

The WebSocket service reads today's candles from SQLite at:

```text
data/live/candles.db
```

Full live path:

```text
IBKR tick producer
  -> market.trades.raw
  -> OneMinuteCandleService
  -> market.candles.1m
  -> CandleSqliteStorageService
  -> CandleWebSocketService
  -> React dashboard
```

## What to notice

- Kafka stores messages in topics.
- Topics are split into partitions.
- A producer writes records with a key and value.
- Records with the same key go to the same partition.
- A consumer reads records as part of a consumer group.
- Offsets are the consumer group's position in each partition.

## Useful commands

Read from the topic with Kafka's CLI:

```sh
docker exec -it stox-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic learning-events \
  --from-beginning
```

Describe the topic:

```sh
docker exec stox-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic learning-events
```

Inspect consumer groups:

```sh
docker exec stox-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group learning-consumer
```

Stop Kafka:

```sh
docker compose down
```
