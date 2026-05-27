import React, { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  CandlestickSeries,
  HistogramSeries,
  ColorType,
  createChart,
  type IChartApi,
  type ISeriesApi,
  type Time,
  type UTCTimestamp
} from "lightweight-charts";
import { Activity, Circle, Clock, Wifi, WifiOff } from "lucide-react";
import "./styles.css";

type CandleMessage = {
  event: string;
  exchange?: string;
  symbol?: string;
  currency?: string;
  timeframe?: string;
  openTime?: string;
  closeTime?: string;
  open?: string;
  high?: string;
  low?: string;
  close?: string;
  volume?: string;
  tickCount?: number;
  isFinal?: boolean;
  publishedAt?: string;
};

type CandleHistoryResponse = CandleMessage[];

type CandlePoint = {
  time: UTCTimestamp;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

type VolumePoint = {
  time: UTCTimestamp;
  value: number;
  color: string;
};

const defaultSymbol = "ASML";
const defaultTimeframe = "1m";
const dateLabelFormatter = new Intl.DateTimeFormat(undefined, {
  weekday: "long",
  year: "numeric",
  month: "long",
  day: "numeric"
});
const axisTimeFormatter = new Intl.DateTimeFormat(undefined, {
  hour: "2-digit",
  minute: "2-digit",
  hour12: false
});

function parseCandle(message: CandleMessage): CandlePoint | null {
  if (!message.openTime || !message.open || !message.high || !message.low || !message.close) {
    return null;
  }

  const time = Math.floor(Date.parse(message.openTime) / 1000);
  const open = Number(message.open);
  const high = Number(message.high);
  const low = Number(message.low);
  const close = Number(message.close);

  if (![time, open, high, low, close].every(Number.isFinite)) {
    return null;
  }

  return {
    time: time as UTCTimestamp,
    open,
    high,
    low,
    close,
    volume: Number(message.volume ?? "0") || 0
  };
}

function parseVolumePoint(candle: CandlePoint): VolumePoint {
  return {
    time: candle.time,
    value: candle.volume,
    color: candle.close >= candle.open ? "#26a69a" : "#ef5350"
  };
}

function formatAxisTime(time: Time) {
  if (typeof time === "number") {
    return axisTimeFormatter.format(new Date(time * 1000));
  }

  if (typeof time === "object" && time !== null && "year" in time && "month" in time && "day" in time) {
    const month = String(time.month).padStart(2, "0");
    const day = String(time.day).padStart(2, "0");
    return `${time.year}-${month}-${day}`;
  }

  return "";
}

function formatDateLabel(value?: string) {
  if (!value) return "No date";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "No date";
  return dateLabelFormatter.format(parsed);
}

function formatTime(value?: string) {
  if (!value) return "No updates";
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }).format(new Date(value));
}

function App() {
  const chartContainerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<"Histogram"> | null>(null);
  const wsRef = useRef<WebSocket | null>(null);

  const [symbol, setSymbol] = useState(defaultSymbol);
  const [status, setStatus] = useState<"connecting" | "open" | "closed">("connecting");
  const [lastMessage, setLastMessage] = useState<CandleMessage | null>(null);
  const [messageCount, setMessageCount] = useState(0);
  const [chartDate, setChartDate] = useState<string | null>(null);

  const wsUrl = useMemo(() => {
    const params = new URLSearchParams({ symbol, timeframe: defaultTimeframe });
    return `ws://localhost:9000/ws/candles?${params.toString()}`;
  }, [symbol]);

  useEffect(() => {
    if (!chartContainerRef.current) return;

    const chart = createChart(chartContainerRef.current, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: "#101418" },
        textColor: "#d8e0e7"
      },
      grid: {
        vertLines: { color: "#202830" },
        horzLines: { color: "#202830" }
      },
      crosshair: {
        mode: 1
      },
      rightPriceScale: {
        borderColor: "#303a44"
      },
      timeScale: {
        borderColor: "#303a44",
        timeVisible: true,
        secondsVisible: false,
        tickMarkFormatter: formatAxisTime
      }
    });

    const series = chart.addSeries(CandlestickSeries, {
      upColor: "#26a69a",
      downColor: "#ef5350",
      borderVisible: false,
      wickUpColor: "#26a69a",
      wickDownColor: "#ef5350"
    });
    const volumeSeries = chart.addSeries(HistogramSeries, {
      color: "#26a69a",
      base: 0,
      priceScaleId: "volume",
      priceFormat: {
        type: "volume"
      }
    });

    chart.priceScale("volume").applyOptions({
      scaleMargins: {
        top: 0.8,
        bottom: 0
      },
      borderVisible: false,
      visible: false,
      ticksVisible: false
    });

    chartRef.current = chart;
    candleSeriesRef.current = series;
    volumeSeriesRef.current = volumeSeries;

    return () => {
      chart.remove();
      chartRef.current = null;
      candleSeriesRef.current = null;
      volumeSeriesRef.current = null;
    };
  }, []);

  useEffect(() => {
    setStatus("connecting");
    setMessageCount(0);
    setLastMessage(null);
    setChartDate(null);
    candleSeriesRef.current?.setData([]);
    volumeSeriesRef.current?.setData([]);

    const abortController = new AbortController();
    let websocket: WebSocket | null = null;

    async function loadHistory() {
      const params = new URLSearchParams({
        symbol,
        timeframe: defaultTimeframe
      });
      const response = await fetch(`http://localhost:9000/api/candles?${params.toString()}`, {
        signal: abortController.signal
      });
      if (!response.ok) {
        return;
      }

      const candles = (await response.json()) as CandleHistoryResponse;
      const points = candles.map(parseCandle).filter((value): value is CandlePoint => value !== null);
      const volumePoints = points.map(parseVolumePoint);
      candleSeriesRef.current?.setData(points);
      volumeSeriesRef.current?.setData(volumePoints);
      if (points.length > 0) {
        chartRef.current?.timeScale().fitContent();
        const last = candles[candles.length - 1];
        if (last) {
          setLastMessage(last);
          setMessageCount(candles.length);
          setChartDate(last.openTime ?? null);
        }
      }
    }

    (async () => {
      await loadHistory().catch(() => {
        // If history fails, the live stream still fills in.
      });

      if (abortController.signal.aborted) return;

      websocket = new WebSocket(wsUrl);
      wsRef.current = websocket;

      websocket.onopen = () => setStatus("open");
      websocket.onclose = () => setStatus("closed");
      websocket.onerror = () => setStatus("closed");
      websocket.onmessage = (event) => {
        const parsed = JSON.parse(event.data) as CandleMessage;
        if (parsed.event === "websocket.connected") return;

        const candle = parseCandle(parsed);
        if (!candle) return;

        candleSeriesRef.current?.update(candle);
        volumeSeriesRef.current?.update(parseVolumePoint(candle));
        chartRef.current?.timeScale().scrollToRealTime();
        setLastMessage(parsed);
        setMessageCount((count) => count + 1);
        if (parsed.openTime) {
          setChartDate(parsed.openTime);
        }
      };
    })();

    return () => {
      abortController.abort();
      websocket?.close();
    };
  }, [wsUrl]);

  const latestPrice = lastMessage?.close ? Number(lastMessage.close) : undefined;
  const statusLabel = status === "open" ? "Live" : status === "connecting" ? "Connecting" : "Disconnected";

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <h1>Stox Kafka</h1>
          <p>{symbol} live 1-minute candles</p>
          <div className="chart-date">{formatDateLabel(chartDate ?? lastMessage?.openTime)}</div>
        </div>

        <div className="controls">
          <label className="symbol-picker">
            <span>Symbol</span>
            <select value={symbol} onChange={(event) => setSymbol(event.target.value)}>
              <option value="ASML">ASML</option>
              <option value="SAP">SAP</option>
              <option value="SIE">SIE</option>
            </select>
          </label>
        </div>
      </header>

      <section className="metrics" aria-label="Live market metrics">
        <Metric
          icon={status === "open" ? <Wifi size={18} /> : <WifiOff size={18} />}
          label="Connection"
          value={statusLabel}
          tone={status === "open" ? "good" : "warn"}
        />
        <Metric
          icon={<Activity size={18} />}
          label="Last price"
          value={latestPrice === undefined ? "-" : latestPrice.toFixed(2)}
        />
        <Metric
          icon={<Clock size={18} />}
          label="Last update"
          value={formatTime(lastMessage?.publishedAt)}
        />
        <Metric
          icon={<Circle size={18} />}
          label="Messages"
          value={String(messageCount)}
        />
      </section>

      <section className="chart-section">
        <div ref={chartContainerRef} className="chart" />
      </section>
    </main>
  );
}

function Metric({
  icon,
  label,
  value,
  tone
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  tone?: "good" | "warn";
}) {
  return (
    <div className="metric">
      <div className={`metric-icon ${tone ?? ""}`}>{icon}</div>
      <div>
        <div className="metric-label">{label}</div>
        <div className="metric-value">{value}</div>
      </div>
    </div>
  );
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
