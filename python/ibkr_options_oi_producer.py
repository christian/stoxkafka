#!/usr/bin/env python3
import argparse
import json
import math
import time
from datetime import datetime, timezone

from ib_async import IB, Option, Stock
from kafka import KafkaProducer


def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def clean_number(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def clean_int(value):
    value = clean_number(value)
    if value is None:
        return None
    return int(value)


def clean_float(value):
    value = clean_number(value)
    if value is None:
        return None
    return float(value)


def format_timestamp(value):
    if value is None or not hasattr(value, "isoformat"):
        return None
    return value.isoformat().replace("+00:00", "Z")


def market_price(ticker):
    if hasattr(ticker, "marketPrice"):
        try:
            value = clean_float(ticker.marketPrice())
            if value is not None:
                return value
        except Exception:
            pass

    for field in ("last", "close", "bid", "ask"):
        value = clean_float(getattr(ticker, field, None))
        if value is not None:
            return value

    return None


def parse_args():
    parser = argparse.ArgumentParser(
        description="Publish IBKR option open-interest snapshots into Kafka."
    )
    parser.add_argument("--ib-host", default="127.0.0.1")
    parser.add_argument(
        "--ib-port",
        type=int,
        default=7497,
        help="7497 for TWS paper, 7496 for TWS live, 4002 for Gateway paper, 4001 for Gateway live",
    )
    parser.add_argument("--ib-client-id", type=int, default=32)
    parser.add_argument("--kafka-bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="market.options.oi.snapshot")
    parser.add_argument("--symbol", default="ASML")
    parser.add_argument("--exchange", default="SMART")
    parser.add_argument("--primary-exchange", default="AEB")
    parser.add_argument("--currency", default="EUR")
    parser.add_argument(
        "--option-exchange",
        default="",
        help="Preferred options exchange from the IBKR chain metadata. Leave blank to use the chain's exchange.",
    )
    parser.add_argument(
        "--market-data-type",
        type=int,
        default=1,
        choices=[1, 2, 3, 4],
        help="1 live, 2 frozen, 3 delayed, 4 delayed frozen",
    )
    parser.add_argument(
        "--max-expiries",
        type=int,
        default=2,
        help="Number of nearest expiries to snapshot. Use 0 for all expiries.",
    )
    parser.add_argument(
        "--strikes-per-expiry",
        type=int,
        default=12,
        help="Number of strikes around spot to snapshot per expiry. Use 0 for all strikes.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=24,
        help="How many option contracts to request in one IBKR snapshot batch.",
    )
    parser.add_argument(
        "--batch-pause-ms",
        type=int,
        default=150,
        help="Pause between snapshot batches to stay friendly with IBKR pacing.",
    )
    return parser.parse_args()


def select_chain(chains, preferred_exchange="", preferred_trading_class=""):
    if not chains:
        raise RuntimeError("IBKR did not return any option chains")

    preferred_exchange = (preferred_exchange or "").strip().upper()
    preferred_trading_class = (preferred_trading_class or "").strip()

    if preferred_exchange:
        for chain in chains:
            if (chain.exchange or "").upper() == preferred_exchange:
                return chain

    if preferred_trading_class:
        for chain in chains:
            if chain.tradingClass == preferred_trading_class:
                return chain

    return max(chains, key=lambda chain: (len(chain.expirations), len(chain.strikes)))


def select_expiries(expirations, max_expiries):
    expirations = sorted({str(expiry) for expiry in expirations})
    if max_expiries and max_expiries > 0:
        return expirations[:max_expiries]
    return expirations


def select_strikes(strikes, underlying_price, strikes_per_expiry):
    strikes = sorted({float(strike) for strike in strikes})
    if strikes_per_expiry is None or strikes_per_expiry <= 0 or len(strikes) <= strikes_per_expiry:
        return strikes

    if underlying_price is None:
        return strikes[:strikes_per_expiry]

    center = min(range(len(strikes)), key=lambda index: abs(strikes[index] - underlying_price))
    half_window = strikes_per_expiry // 2
    start = max(0, center - half_window)
    end = start + strikes_per_expiry

    if end > len(strikes):
        end = len(strikes)
        start = max(0, end - strikes_per_expiry)

    return strikes[start:end]


def option_open_interest(ticker, right):
    right = (right or "").upper()
    call_oi = clean_int(getattr(ticker, "callOpenInterest", None))
    put_oi = clean_int(getattr(ticker, "putOpenInterest", None))
    primary = call_oi if right == "C" else put_oi
    return primary, call_oi, put_oi


def build_option_contracts(args, chain, underlying_price):
    expiries = select_expiries(chain.expirations, args.max_expiries)
    strikes = select_strikes(chain.strikes, underlying_price, args.strikes_per_expiry)

    option_exchange = (args.option_exchange or chain.exchange or "SMART").strip()
    contracts = []
    for expiry in expiries:
        for strike in strikes:
            for right in ("C", "P"):
                contracts.append(
                    Option(
                        args.symbol,
                        expiry,
                        strike,
                        right,
                        exchange=option_exchange,
                        multiplier=chain.multiplier,
                        currency=args.currency,
                        tradingClass=chain.tradingClass,
                    )
                )

    return contracts


def snapshot_from_option_ticker(ticker, contract, chain, underlying_contract, underlying_price, snapshot_at):
    primary_oi, call_oi, put_oi = option_open_interest(ticker, contract.right)
    return {
        "event": "ibkr.option_oi_snapshot",
        "exchange": "ibkr",
        "source": f"ibkr:{underlying_contract.symbol}:{underlying_contract.currency}:{underlying_contract.primaryExchange or underlying_contract.exchange}",
        "snapshotAt": snapshot_at,
        "underlyingSymbol": underlying_contract.symbol,
        "underlyingCurrency": underlying_contract.currency,
        "underlyingExchange": underlying_contract.exchange,
        "underlyingPrimaryExchange": getattr(underlying_contract, "primaryExchange", None),
        "underlyingConId": getattr(underlying_contract, "conId", None),
        "underlyingPrice": underlying_price,
        "symbol": contract.symbol,
        "currency": contract.currency,
        "optionExchange": contract.exchange,
        "chainExchange": chain.exchange,
        "tradingClass": chain.tradingClass,
        "multiplier": chain.multiplier,
        "conId": getattr(contract, "conId", None),
        "expiry": contract.lastTradeDateOrContractMonth,
        "strike": clean_float(contract.strike),
        "right": contract.right,
        "optionOpenInterest": primary_oi,
        "callOpenInterest": call_oi,
        "putOpenInterest": put_oi,
        "bid": clean_float(getattr(ticker, "bid", None)),
        "ask": clean_float(getattr(ticker, "ask", None)),
        "last": clean_float(getattr(ticker, "last", None)),
        "close": clean_float(getattr(ticker, "close", None)),
        "volume": clean_int(getattr(ticker, "volume", None)),
        "openInterest": primary_oi,
        "underlyingMid": underlying_price,
        "sourceTimestamp": format_timestamp(getattr(ticker, "lastTimestamp", None)),
        "publishedAt": now_iso(),
    }


def publish_batch(ib, producer, args, chain, underlying_contract, contracts, underlying_price, snapshot_at):
    sent = 0
    for batch_start in range(0, len(contracts), args.batch_size):
        batch = contracts[batch_start : batch_start + args.batch_size]
        tickers = ib.reqTickers(*batch)

        for contract, ticker in zip(batch, tickers):
            event = snapshot_from_option_ticker(
                ticker,
                contract,
                chain,
                underlying_contract,
                underlying_price,
                snapshot_at,
            )

            if (
                event["optionOpenInterest"] is None
                and event["callOpenInterest"] is None
                and event["putOpenInterest"] is None
            ):
                continue

            key = f"ibkr:{underlying_contract.symbol}:{contract.lastTradeDateOrContractMonth}:{contract.strike}:{contract.right}"
            producer.send(args.topic, key=key, value=event)
            sent += 1

            print(
                f"sent key={key} oi={event['optionOpenInterest']} "
                f"bid={event['bid']} ask={event['ask']} last={event['last']}"
            )

        if batch_start + args.batch_size < len(contracts) and args.batch_pause_ms > 0:
            time.sleep(args.batch_pause_ms / 1000.0)

    return sent


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
    try:
        print(f"Connecting to IBKR at {args.ib_host}:{args.ib_port} clientId={args.ib_client_id}")
        ib.connect(args.ib_host, args.ib_port, clientId=args.ib_client_id)
        ib.reqMarketDataType(args.market_data_type)

        underlying_contract = Stock(
            args.symbol,
            args.exchange,
            args.currency,
            primaryExchange=args.primary_exchange,
        )
        qualified_underlying = ib.qualifyContracts(underlying_contract)
        if not qualified_underlying:
            raise RuntimeError(f"IBKR did not qualify underlying contract: {underlying_contract}")

        underlying_contract = qualified_underlying[0]
        underlying_tickers = ib.reqTickers(underlying_contract)
        if not underlying_tickers:
            raise RuntimeError(f"IBKR did not return a market data snapshot for {args.symbol}")

        underlying_ticker = underlying_tickers[0]
        underlying_price = market_price(underlying_ticker)

        chains = ib.reqSecDefOptParams(
            underlying_contract.symbol,
            "",
            "STK",
            underlying_contract.conId,
        )
        chain = select_chain(chains, args.option_exchange, getattr(underlying_contract, "tradingClass", ""))

        contracts = build_option_contracts(args, chain, underlying_price)
        qualified_contracts = []
        if contracts:
            qualified_contracts = [contract for contract in ib.qualifyContracts(*contracts) if contract is not None]

        print(
            f"Publishing option OI snapshots for {args.symbol} "
            f"underlyingPrice={underlying_price} chain={chain.exchange}/{chain.tradingClass} "
            f"contracts={len(qualified_contracts)} topic={args.topic}"
        )

        snapshot_at = now_iso()

        sent = publish_batch(
            ib,
            producer,
            args,
            chain,
            underlying_contract,
            qualified_contracts,
            underlying_price,
            snapshot_at,
        )

        producer.flush(10)
        print(f"Completed snapshot: sent={sent} contracts={len(qualified_contracts)}")
    finally:
        try:
            producer.flush(5)
            producer.close(5)
        except Exception:
            pass
        if ib.isConnected():
            ib.disconnect()


if __name__ == "__main__":
    main()
