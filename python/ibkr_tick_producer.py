#!/usr/bin/env python3
import argparse
import json
import math
import signal
import sys
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from ib_async import IB, Stock
from kafka import KafkaProducer


def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def clean_number(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def clean_size(value):
    value = clean_number(value)
    if value is None:
        return None
    return int(value)


def snapshot_from_ticker(ticker, contract, source):
    return {
        "event": "ibkr.market_tick",
        "source": source,
        "exchange": "ibkr",
        "symbol": contract.symbol,
        "currency": contract.currency,
        "ibExchange": contract.exchange,
        "primaryExchange": getattr(contract, "primaryExchange", None),
        "conId": getattr(contract, "conId", None),
        "bid": clean_number(ticker.bid),
        "bidSize": clean_size(ticker.bidSize),
        "ask": clean_number(ticker.ask),
        "askSize": clean_size(ticker.askSize),
        "last": clean_number(ticker.last),
        "lastSize": clean_size(ticker.lastSize),
        "close": clean_number(ticker.close),
        "volume": clean_size(ticker.volume),
        "sourceTimestamp": ticker.time.isoformat().replace("+00:00", "Z") if ticker.time else None,
        "publishedAt": now_iso(),
    }


def compact_signature(event):
    return (
        event["bid"],
        event["bidSize"],
        event["ask"],
        event["askSize"],
        event["last"],
        event["lastSize"],
        event["close"],
        event["volume"],
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Stream one IBKR European stock ticker into Kafka."
    )
    parser.add_argument("--ib-host", default="127.0.0.1")
    parser.add_argument("--ib-port", type=int, default=7497, help="7497 for TWS paper, 7496 for TWS live, 4002 for Gateway paper, 4001 for Gateway live")
    parser.add_argument("--ib-client-id", type=int, default=31)
    parser.add_argument("--kafka-bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="market.trades.raw")
    parser.add_argument("--symbol", default="ASML")
    parser.add_argument("--exchange", default="SMART")
    parser.add_argument("--primary-exchange", default="AEB", help="AEB is Euronext Amsterdam in IBKR")
    parser.add_argument("--currency", default="EUR")
    parser.add_argument("--market-data-type", type=int, default=1, choices=[1, 2, 3, 4], help="1 live, 2 frozen, 3 delayed, 4 delayed frozen")
    parser.add_argument("--min-interval-ms", type=int, default=250)
    parser.add_argument(
        "--trading-timezone",
        default="Europe/Amsterdam",
        help="IANA timezone used to classify premarket/regular/after-hours session labels.",
    )
    parser.add_argument(
        "--regular-open",
        default="09:00",
        help="Local exchange open time in HH:MM used for session labeling.",
    )
    parser.add_argument(
        "--regular-close",
        default="17:30",
        help="Local exchange close time in HH:MM used for session labeling.",
    )
    return parser.parse_args()


def parse_hhmm(value):
    hour, minute = value.split(":", 1)
    return int(hour), int(minute)


def normalize_utc(value):
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def classify_session(source_timestamp_utc, timezone_name, regular_open, regular_close):
    if source_timestamp_utc is None:
        return "unknown", False

    source_timestamp_utc = normalize_utc(source_timestamp_utc)
    trading_timezone = ZoneInfo(timezone_name)
    local_time = source_timestamp_utc.astimezone(trading_timezone)
    open_hour, open_minute = parse_hhmm(regular_open)
    close_hour, close_minute = parse_hhmm(regular_close)

    open_time = local_time.replace(hour=open_hour, minute=open_minute, second=0, microsecond=0)
    close_time = local_time.replace(hour=close_hour, minute=close_minute, second=0, microsecond=0)

    if local_time < open_time:
        return "premarket", True
    if local_time <= close_time:
        return "regular", False
    return "afterhours", False


def main():
    args = parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.kafka_bootstrap,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        acks="all",
        linger_ms=5,
    )

    ib = IB()
    print(f"Connecting to IBKR at {args.ib_host}:{args.ib_port} clientId={args.ib_client_id}")
    ib.connect(args.ib_host, args.ib_port, clientId=args.ib_client_id)
    ib.reqMarketDataType(args.market_data_type)

    contract = Stock(
        args.symbol,
        args.exchange,
        args.currency,
        primaryExchange=args.primary_exchange,
    )
    qualified = ib.qualifyContracts(contract)
    if not qualified:
        raise RuntimeError(f"IBKR did not qualify contract: {contract}")

    contract = qualified[0]
    key = f"ibkr:{contract.symbol}"
    source = f"ibkr:{contract.symbol}:{contract.currency}:{contract.primaryExchange or contract.exchange}"
    print(f"Streaming {source} into Kafka topic={args.topic} key={key}")

    ticker = ib.reqMktData(contract, "", False, False)
    last_signature = None
    last_publish_at = 0.0

    def publish_tick(updated_ticker):
        nonlocal last_signature, last_publish_at

        source_timestamp = updated_ticker.time
        if source_timestamp is None:
            source_timestamp = datetime.now(timezone.utc)
        session, is_premarket = classify_session(
            source_timestamp,
            args.trading_timezone,
            args.regular_open,
            args.regular_close,
        )
        event = snapshot_from_ticker(updated_ticker, contract, source)
        event["session"] = session
        event["isPremarket"] = is_premarket
        signature = compact_signature(event)
        monotonic_now = time.monotonic()

        if signature == last_signature:
            return

        elapsed_ms = (monotonic_now - last_publish_at) * 1000
        if elapsed_ms < args.min_interval_ms:
            return

        producer.send(args.topic, key=key, value=event)
        last_signature = signature
        last_publish_at = monotonic_now

        print(
            f"sent key={key} session={session} bid={event['bid']} ask={event['ask']} "
            f"last={event['last']} volume={event['volume']}"
        )

    ticker.updateEvent += publish_tick

    def stop(_signal_number=None, _frame=None):
        print("Stopping")
        ib.cancelMktData(contract)
        producer.flush(5)
        producer.close(5)
        ib.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    try:
        ib.run()
    finally:
        if ib.isConnected():
            stop()


if __name__ == "__main__":
    main()
