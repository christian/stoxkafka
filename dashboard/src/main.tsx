import React, { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  CandlestickSeries,
  ColorType,
  createChart,
  type IChartApi,
  type ISeriesApi,
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

type CandlePoint = {
  time: UTCTimestamp;
  open: number;
  high: number;
  low: number;
  close: number;
};

const defaultSymbol = "ASML";
const defaultTimeframe = "1m";

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
    close
  };
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
  const wsRef = useRef<WebSocket | null>(null);

  const [symbol, setSymbol] = useState(defaultSymbol);
  const [status, setStatus] = useState<"connecting" | "open" | "closed">("connecting");
  const [lastMessage, setLastMessage] = useState<CandleMessage | null>(null);
  const [messageCount, setMessageCount] = useState(0);

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
        secondsVisible: false
      }
    });

    const series = chart.addSeries(CandlestickSeries, {
      upColor: "#26a69a",
      downColor: "#ef5350",
      borderVisible: false,
      wickUpColor: "#26a69a",
      wickDownColor: "#ef5350"
    });

    chartRef.current = chart;
    candleSeriesRef.current = series;

    return () => {
      chart.remove();
      chartRef.current = null;
      candleSeriesRef.current = null;
    };
  }, []);

  useEffect(() => {
    setStatus("connecting");
    setMessageCount(0);
    setLastMessage(null);
    candleSeriesRef.current?.setData([]);

    const websocket = new WebSocket(wsUrl);
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
      chartRef.current?.timeScale().scrollToRealTime();
      setLastMessage(parsed);
      setMessageCount((count) => count + 1);
    };

    return () => {
      websocket.close();
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

