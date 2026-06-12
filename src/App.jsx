import { useState, useEffect, useRef, useCallback } from "react";

// ─── constants ────────────────────────────────────────────────────────────────
const MAX_HISTORY = 500;
const DIGITS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

const SYMBOLS = [
  { group: "Volatility (1s)", options: [
    { label: "Volatility 10 (1s)",  value: "1HZ10V" },
    { label: "Volatility 25 (1s)",  value: "1HZ25V" },
    { label: "Volatility 50 (1s)",  value: "1HZ50V" },
    { label: "Volatility 75 (1s)",  value: "1HZ75V" },
    { label: "Volatility 100 (1s)", value: "1HZ100V" },
    { label: "Volatility 150 (1s)", value: "1HZ150V" },
    { label: "Volatility 200 (1s)", value: "1HZ200V" },
    { label: "Volatility 250 (1s)", value: "1HZ250V" },
    { label: "Volatility 300 (1s)", value: "1HZ300V" },
  ]},
  { group: "Volatility", options: [
    { label: "Volatility 10",  value: "R_10" },
    { label: "Volatility 25",  value: "R_25" },
    { label: "Volatility 50",  value: "R_50" },
    { label: "Volatility 75",  value: "R_75" },
    { label: "Volatility 100", value: "R_100" },
  ]},
  { group: "Crash/Boom", options: [
    { label: "Crash 300",   value: "CRASH300N" },
    { label: "Crash 500",   value: "CRASH500" },
    { label: "Crash 1000",  value: "CRASH1000" },
    { label: "Boom 300",    value: "BOOM300N" },
    { label: "Boom 500",    value: "BOOM500" },
    { label: "Boom 1000",   value: "BOOM1000" },
  ]},
  { group: "Step Index", options: [
    { label: "Step Index", value: "stpRNG" },
  ]},
  { group: "Range Break", options: [
    { label: "Range Break 100", value: "RNGBR100N" },
    { label: "Range Break 200", value: "RNGBR200N" },
  ]},
  { group: "Jump", options: [
    { label: "Jump 10",  value: "JD10" },
    { label: "Jump 25",  value: "JD25" },
    { label: "Jump 50",  value: "JD50" },
    { label: "Jump 75",  value: "JD75" },
    { label: "Jump 100", value: "JD100" },
  ]},
];

const WINDOW_SIZES = [20, 50, 100, 200];
const SIGNAL_THRESHOLD = 13.5; // % above expected 10% → strong match signal
const DIFFERS_THRESHOLD = 7.0;  // % below expected 10% → strong differs signal

// ─── helpers ──────────────────────────────────────────────────────────────────
const getLastDigit = (price) => {
  const s = price.toFixed(5);
  return parseInt(s[s.length - 1]);
};

const buildFreq = (arr) => {
  const f = Object.fromEntries(DIGITS.map(d => [d, 0]));
  arr.forEach(d => f[d]++);
  return f;
};

const pct = (count, total) => total ? (count / total) * 100 : 0;

// Streak: how many ticks since digit last appeared
const getStreaks = (history) => {
  const last = Object.fromEntries(DIGITS.map(d => [d, null]));
  history.forEach((d, i) => { last[d] = i; });
  const now = history.length - 1;
  return Object.fromEntries(DIGITS.map(d => [
    d,
    last[d] === null ? history.length : now - last[d]
  ]));
};

// Signal scoring per digit
const scoreDigit = (digit, freq, total, streaks, windowFreq, windowTotal) => {
  const globalPct = pct(freq[digit], total);
  const windowPct = pct(windowFreq[digit], windowTotal);
  const streak = streaks[digit]; // ticks since last seen

  // MATCH signal: digit is over-represented
  let matchScore = 0;
  if (globalPct > 10) matchScore += (globalPct - 10) * 2;
  if (windowPct > 10) matchScore += (windowPct - 10) * 3; // window weighted more
  if (streak <= 3) matchScore += 5; // recently active

  // DIFFERS signal: digit is under-represented / overdue relief
  let differsScore = 0;
  if (globalPct < 10) differsScore += (10 - globalPct) * 2;
  if (windowPct < 10) differsScore += (10 - windowPct) * 3;
  if (streak >= 15) differsScore += Math.min((streak - 10) * 2, 20); // drought bonus

  return { matchScore, differsScore, globalPct, windowPct, streak };
};

// ─── Deriv WebSocket hook ─────────────────────────────────────────────────────
function useDerivStream(symbol, enabled) {
  const [history, setHistory] = useState([]);
  const [lastPrice, setLastPrice] = useState(null);
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState(null);
  const wsRef = useRef(null);

  const stop = useCallback(() => {
    if (wsRef.current) { wsRef.current.close(); wsRef.current = null; }
    setConnected(false);
  }, []);

  useEffect(() => {
    if (!enabled) { stop(); return; }
    setHistory([]); setLastPrice(null); setError(null);
    const ws = new WebSocket("wss://ws.binaryws.com/websockets/v3?app_id=1089");
    wsRef.current = ws;
    ws.onopen = () => {
      setConnected(true);
      ws.send(JSON.stringify({ ticks: symbol, subscribe: 1 }));
    };
    ws.onmessage = (e) => {
      const msg = JSON.parse(e.data);
      if (msg.error) { setError(msg.error.message); return; }
      if (msg.tick) {
        const price = msg.tick.quote;
        setLastPrice(price);
        setHistory(prev => {
          const next = [...prev, getLastDigit(price)];
          return next.length > MAX_HISTORY ? next.slice(-MAX_HISTORY) : next;
        });
      }
    };
    ws.onerror = (e) => { console.error("WS error:", e); setError("Connection failed — check console for details."); };
    ws.onclose = () => setConnected(false);
    return stop;
  }, [symbol, enabled, stop]);

  return { history, lastPrice, connected, error, reset: () => setHistory([]) };
}

// ─── sub-components ───────────────────────────────────────────────────────────

function PredictionCard({ digit, type, score, globalPct, windowPct, streak, rank }) {
  const isMatch = type === "MATCH";
  const accent = isMatch ? "#a78bfa" : "#f472b6";
  const bgAccent = isMatch ? "#a78bfa18" : "#f472b618";
  const borderAccent = isMatch ? "#a78bfa40" : "#f472b640";
  const label = isMatch ? "MATCHES" : "DIFFERS";

  return (
    <div style={{
      background: bgAccent,
      border: `1px solid ${borderAccent}`,
      borderRadius: "10px",
      padding: "16px",
      position: "relative",
      overflow: "hidden",
    }}>
      {rank === 1 && (
        <div style={{
          position: "absolute", top: "8px", right: "10px",
          fontSize: "9px", letterSpacing: "0.15em", textTransform: "uppercase",
          color: accent, opacity: 0.7
        }}>TOP PICK</div>
      )}
      <div style={{ fontSize: "9px", color: accent, letterSpacing: "0.15em", textTransform: "uppercase", marginBottom: "6px" }}>
        {label} · #{rank}
      </div>
      <div style={{
        fontSize: "56px", fontWeight: "800", fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
        color: accent, lineHeight: 1, marginBottom: "10px"
      }}>{digit}</div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "6px" }}>
        {[
          { label: "Global", val: `${globalPct.toFixed(1)}%`, highlight: isMatch ? globalPct > 10 : globalPct < 10 },
          { label: "Window", val: `${windowPct.toFixed(1)}%`, highlight: isMatch ? windowPct > 10 : windowPct < 10 },
          { label: "Drought", val: `${streak}t`, highlight: isMatch ? streak <= 4 : streak >= 12 },
        ].map(({ label, val, highlight }) => (
          <div key={label} style={{
            background: highlight ? `${accent}22` : "#0f172a",
            borderRadius: "6px", padding: "6px 8px", textAlign: "center"
          }}>
            <div style={{ fontSize: "9px", color: "#64748b", textTransform: "uppercase", letterSpacing: "0.08em" }}>{label}</div>
            <div style={{ fontSize: "13px", fontFamily: "monospace", fontWeight: "700", color: highlight ? accent : "#94a3b8", marginTop: "1px" }}>{val}</div>
          </div>
        ))}
      </div>
      {/* score bar */}
      <div style={{ marginTop: "10px" }}>
        <div style={{ display: "flex", justifyContent: "space-between", fontSize: "9px", color: "#475569", marginBottom: "3px" }}>
          <span>Signal strength</span><span>{Math.min(score, 100).toFixed(0)}/100</span>
        </div>
        <div style={{ height: "4px", background: "#0f172a", borderRadius: "2px", overflow: "hidden" }}>
          <div style={{
            height: "100%", width: `${Math.min(score, 100)}%`,
            background: `linear-gradient(90deg, ${accent}80, ${accent})`,
            borderRadius: "2px", transition: "width 0.5s ease"
          }} />
        </div>
      </div>
    </div>
  );
}

function DigitGrid({ freq, total, windowFreq, windowTotal, streaks, matchPick, differsPick }) {
  return (
    <div style={{
      display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: "6px"
    }}>
      {DIGITS.map(d => {
        const g = pct(freq[d], total);
        const w = pct(windowFreq[d], windowTotal);
        const s = streaks[d];
        const isMatch = d === matchPick;
        const isDiff = d === differsPick;
        const gDev = g - 10;
        const barColor = isMatch ? "#a78bfa" : isDiff ? "#f472b6" : gDev > 2 ? "#f59e0b" : gDev < -2 ? "#38bdf8" : "#334155";

        return (
          <div key={d} style={{
            background: isMatch ? "#a78bfa12" : isDiff ? "#f472b812" : "#0a0f1e",
            border: `1px solid ${isMatch ? "#a78bfa40" : isDiff ? "#f472b840" : "#1e293b"}`,
            borderRadius: "8px", padding: "8px 6px", textAlign: "center"
          }}>
            <div style={{
              fontSize: "18px", fontFamily: "monospace", fontWeight: "800",
              color: isMatch ? "#a78bfa" : isDiff ? "#f472b6" : "#64748b",
              lineHeight: 1
            }}>{d}</div>
            {/* bar */}
            <div style={{ height: "3px", background: "#0f172a", borderRadius: "2px", margin: "5px 0 4px", overflow: "hidden" }}>
              <div style={{ height: "100%", width: `${Math.min(g * 4, 100)}%`, background: barColor, borderRadius: "2px", transition: "width 0.4s" }} />
            </div>
            <div style={{ fontSize: "10px", fontFamily: "monospace", color: gDev > 1.5 ? "#f59e0b" : gDev < -1.5 ? "#38bdf8" : "#475569" }}>
              {g.toFixed(1)}%
            </div>
            <div style={{ fontSize: "9px", color: "#334155", marginTop: "1px" }}>
              {s === 0 ? "just" : `${s}t ago`}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function TickTape({ history }) {
  const last30 = history.slice(-30);
  return (
    <div style={{ display: "flex", gap: "3px", flexWrap: "wrap" }}>
      {last30.map((d, i) => {
        const isLast = i === last30.length - 1;
        const age = last30.length - 1 - i;
        const alpha = 0.25 + (i / last30.length) * 0.75;
        return (
          <div key={i} style={{
            width: "24px", height: "24px",
            display: "flex", alignItems: "center", justifyContent: "center",
            borderRadius: "4px",
            background: isLast ? "#a78bfa22" : "transparent",
            border: `1px solid ${isLast ? "#a78bfa60" : "#1e293b"}`,
            color: isLast ? "#a78bfa" : `rgba(148,163,184,${alpha})`,
            fontFamily: "monospace", fontSize: "12px", fontWeight: "700",
            transition: "all 0.15s"
          }}>{d}</div>
        );
      })}
    </div>
  );
}

function StreakAlert({ streaks, history }) {
  const alerts = [];
  DIGITS.forEach(d => {
    if (streaks[d] >= 20) alerts.push({ digit: d, streak: streaks[d], type: "drought" });
  });
  // Check recent repeat (same digit 3+ times in last 10)
  const last10 = history.slice(-10);
  DIGITS.forEach(d => {
    const cnt = last10.filter(x => x === d).length;
    if (cnt >= 3) alerts.push({ digit: d, count: cnt, type: "hot" });
  });
  if (alerts.length === 0) return null;
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "6px" }}>
      {alerts.slice(0, 3).map((a, i) => (
        <div key={i} style={{
          display: "flex", alignItems: "center", gap: "10px",
          background: a.type === "drought" ? "#1e1a2e" : "#1e1208",
          border: `1px solid ${a.type === "drought" ? "#a78bfa30" : "#f59e0b30"}`,
          borderRadius: "6px", padding: "8px 12px", fontSize: "11px"
        }}>
          <span style={{ fontSize: "16px" }}>{a.type === "drought" ? "🧊" : "🔥"}</span>
          <span style={{ color: "#94a3b8" }}>
            {a.type === "drought"
              ? <><span style={{ color: "#a78bfa", fontFamily: "monospace", fontWeight: "700" }}>{a.digit}</span> absent for <strong style={{ color: "#e2e8f0" }}>{a.streak} ticks</strong> — overdue for DIFFERS</>
              : <><span style={{ color: "#f59e0b", fontFamily: "monospace", fontWeight: "700" }}>{a.digit}</span> appeared <strong style={{ color: "#e2e8f0" }}>{a.count}×</strong> in last 10 — strong MATCHES signal</>
            }
          </span>
        </div>
      ))}
    </div>
  );
}

// ─── main ─────────────────────────────────────────────────────────────────────
export default function MatchesDiffersPredictor() {
  const [symbol, setSymbol] = useState("1HZ10V");
  const [streaming, setStreaming] = useState(false);
  const [window, setWindow] = useState(50);
  const [manualInput, setManualInput] = useState("");
  const [useManual, setUseManual] = useState(false);
  const [manualHistory, setManualHistory] = useState([]);

  const { history: liveHistory, lastPrice, connected, error, reset } = useDerivStream(symbol, streaming && !useManual);
  const history = useManual ? manualHistory : liveHistory;

  const total = history.length;
  const windowHistory = history.slice(-window);
  const windowTotal = windowHistory.length;

  const freq = buildFreq(history);
  const windowFreq = buildFreq(windowHistory);
  const streaks = total > 0 ? getStreaks(history) : Object.fromEntries(DIGITS.map(d => [d, 0]));

  // Score all digits for both strategies
  const scores = DIGITS.map(d => ({
    digit: d,
    ...scoreDigit(d, freq, total, streaks, windowFreq, windowTotal)
  }));

  const matchRanked = [...scores].sort((a, b) => b.matchScore - a.matchScore);
  const differsRanked = [...scores].sort((a, b) => b.differsScore - a.differsScore);

  const top3Match = matchRanked.slice(0, 3);
  const top3Differs = differsRanked.slice(0, 3);

  const handleManualAdd = () => {
    const digits = manualInput.replace(/[^0-9]/g, "").split("").map(Number);
    if (digits.length > 0) {
      setManualHistory(prev => [...prev, ...digits].slice(-MAX_HISTORY));
      setManualInput("");
    }
  };

  const handleReset = () => {
    if (useManual) setManualHistory([]);
    else reset();
  };

  return (
    <div style={{
      minHeight: "100vh",
      background: "#06030f",
      color: "#e2e8f0",
      fontFamily: "'Inter', system-ui, sans-serif",
      padding: "20px 16px 40px",
    }}>
      <div style={{ maxWidth: "760px", margin: "0 auto" }}>

        {/* ── header ── */}
        <div style={{ marginBottom: "22px" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: "6px" }}>
            <div style={{
              width: "7px", height: "7px", borderRadius: "50%",
              background: connected && !useManual ? "#22c55e" : useManual ? "#f59e0b" : "#ef444460",
              boxShadow: connected && !useManual ? "0 0 10px #22c55e" : "none",
              flexShrink: 0
            }} />
            <span style={{ fontSize: "10px", color: "#475569", letterSpacing: "0.14em", textTransform: "uppercase" }}>
              {useManual ? "Manual" : connected ? `Live · ${SYMBOLS.flatMap(g=>g.options).find(s=>s.value===symbol)?.label}` : "Offline"}
            </span>
            <span style={{ marginLeft: "auto", fontSize: "10px", color: total >= 50 ? "#22c55e" : "#f59e0b", fontFamily: "monospace" }}>
              {total} ticks {total < 50 ? `(need ${50 - total} more)` : ""}
            </span>
          </div>
          <h1 style={{
            margin: 0, fontSize: "20px", fontWeight: "700", letterSpacing: "-0.02em",
            color: "#e2e8f0"
          }}>
            Matches & Differs Predictor
          </h1>
          <p style={{ margin: "3px 0 0", fontSize: "12px", color: "#334155" }}>
            Real-time digit frequency signals for Deriv synthetic indices
          </p>
        </div>

        {/* ── controls ── */}
        <div style={{
          display: "grid", gridTemplateColumns: "1fr 1fr auto", gap: "10px",
          marginBottom: "12px", alignItems: "end"
        }}>
          <div>
            <div style={{ fontSize: "10px", color: "#475569", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: "5px" }}>Symbol</div>
            <select value={symbol} onChange={e => { setSymbol(e.target.value); handleReset(); }}
              disabled={streaming && !useManual}
              style={{ width: "100%", background: "#0d0a1a", border: "1px solid #1e1b2e", color: "#e2e8f0", borderRadius: "6px", padding: "8px 10px", fontSize: "12px" }}>
              {SYMBOLS.map(g => (
                <optgroup key={g.group} label={g.group}>
                  {g.options.map(s => <option key={s.value} value={s.value}>{s.label}</option>)}
                </optgroup>
              ))}
            </select>
          </div>
          <div>
            <div style={{ fontSize: "10px", color: "#475569", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: "5px" }}>Analysis window</div>
            <select value={window} onChange={e => setWindow(Number(e.target.value))}
              style={{ width: "100%", background: "#0d0a1a", border: "1px solid #1e1b2e", color: "#e2e8f0", borderRadius: "6px", padding: "8px 10px", fontSize: "12px" }}>
              {WINDOW_SIZES.map(w => <option key={w} value={w}>Last {w} ticks</option>)}
            </select>
          </div>
          <div style={{ display: "flex", gap: "6px" }}>
            {!useManual && (
              <button onClick={() => setStreaming(s => !s)} style={{
                padding: "8px 18px", borderRadius: "6px", fontSize: "12px", fontWeight: "700",
                cursor: "pointer", border: "none",
                background: streaming ? "#ef4444" : "#7c3aed",
                color: "#fff", whiteSpace: "nowrap"
              }}>{streaming ? "⏹ Stop" : "▶ Stream"}</button>
            )}
            <button onClick={handleReset} style={{
              padding: "8px 12px", borderRadius: "6px", fontSize: "12px",
              cursor: "pointer", border: "1px solid #1e1b2e",
              background: "transparent", color: "#475569"
            }}>↺</button>
          </div>
        </div>

        {/* mode tabs */}
        <div style={{ display: "flex", gap: "4px", marginBottom: "16px" }}>
          {["Live Stream", "Manual Entry"].map((label, i) => (
            <button key={label} onClick={() => { setUseManual(i === 1); if (i === 0 && streaming) {} }}
              style={{
                padding: "6px 14px", borderRadius: "5px", fontSize: "11px", fontWeight: "600",
                cursor: "pointer", border: "1px solid",
                background: useManual === (i === 1) ? "#1e1b2e" : "transparent",
                borderColor: useManual === (i === 1) ? "#7c3aed50" : "#1e1b2e",
                color: useManual === (i === 1) ? "#a78bfa" : "#475569"
              }}>{label}</button>
          ))}
        </div>

        {/* manual entry */}
        {useManual && (
          <div style={{
            background: "#0d0a1a", border: "1px solid #1e1b2e",
            borderRadius: "8px", padding: "14px", marginBottom: "14px"
          }}>
            <div style={{ fontSize: "10px", color: "#475569", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: "8px" }}>
              Paste digits from Deriv history
            </div>
            <div style={{ display: "flex", gap: "8px" }}>
              <input value={manualInput} onChange={e => setManualInput(e.target.value)}
                onKeyDown={e => e.key === "Enter" && handleManualAdd()}
                placeholder="3 7 2 0 5 8 1 9 4 6 …"
                style={{
                  flex: 1, background: "#06030f", border: "1px solid #1e1b2e",
                  color: "#e2e8f0", borderRadius: "6px", padding: "8px 10px",
                  fontSize: "13px", fontFamily: "monospace"
                }} />
              <button onClick={handleManualAdd} style={{
                padding: "8px 16px", borderRadius: "6px", fontSize: "12px", fontWeight: "600",
                cursor: "pointer", border: "none", background: "#7c3aed", color: "#fff"
              }}>Add</button>
            </div>
          </div>
        )}

        {/* error */}
        {error && (
          <div style={{ background: "#1a0505", border: "1px solid #ef444450", borderRadius: "6px", padding: "10px 14px", marginBottom: "14px", fontSize: "12px", color: "#fca5a5" }}>
            ⚠ {error}
          </div>
        )}

        {total === 0 ? (
          <div style={{
            textAlign: "center", padding: "60px 20px",
            color: "#1e1b2e", fontSize: "13px"
          }}>
            <div style={{ fontSize: "36px", marginBottom: "12px" }}>◎</div>
            {useManual ? "Paste digits above and click Add." : "Press Stream to begin collecting ticks."}
          </div>
        ) : (
          <>
            {/* ── MATCHES section ── */}
            <div style={{ marginBottom: "20px" }}>
              <div style={{
                display: "flex", alignItems: "center", gap: "10px",
                marginBottom: "12px", paddingBottom: "8px",
                borderBottom: "1px solid #1e1b2e"
              }}>
                <div style={{
                  background: "#a78bfa20", border: "1px solid #a78bfa40",
                  borderRadius: "4px", padding: "3px 10px",
                  fontSize: "11px", fontWeight: "700", color: "#a78bfa", letterSpacing: "0.1em"
                }}>MATCHES</div>
                <span style={{ fontSize: "11px", color: "#475569" }}>
                  Trade: last digit = your pick
                </span>
                <span style={{ marginLeft: "auto", fontSize: "10px", color: "#334155" }}>
                  Top 3 picks by signal
                </span>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: "10px" }}>
                {top3Match.map((s, i) => (
                  <PredictionCard key={s.digit} digit={s.digit} type="MATCH"
                    score={Math.min(s.matchScore, 100)}
                    globalPct={s.globalPct} windowPct={s.windowPct}
                    streak={s.streak} rank={i + 1} />
                ))}
              </div>
            </div>

            {/* ── DIFFERS section ── */}
            <div style={{ marginBottom: "20px" }}>
              <div style={{
                display: "flex", alignItems: "center", gap: "10px",
                marginBottom: "12px", paddingBottom: "8px",
                borderBottom: "1px solid #1e1b2e"
              }}>
                <div style={{
                  background: "#f472b620", border: "1px solid #f472b640",
                  borderRadius: "4px", padding: "3px 10px",
                  fontSize: "11px", fontWeight: "700", color: "#f472b6", letterSpacing: "0.1em"
                }}>DIFFERS</div>
                <span style={{ fontSize: "11px", color: "#475569" }}>
                  Trade: last digit ≠ your pick
                </span>
                <span style={{ marginLeft: "auto", fontSize: "10px", color: "#334155" }}>
                  Avoid these digits
                </span>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: "10px" }}>
                {top3Differs.map((s, i) => (
                  <PredictionCard key={s.digit} digit={s.digit} type="DIFFERS"
                    score={Math.min(s.differsScore, 100)}
                    globalPct={s.globalPct} windowPct={s.windowPct}
                    streak={s.streak} rank={i + 1} />
                ))}
              </div>
            </div>

            {/* ── Alerts ── */}
            {total >= 20 && (
              <div style={{ marginBottom: "20px" }}>
                <StreakAlert streaks={streaks} history={history} />
              </div>
            )}

            {/* ── digit grid ── */}
            <div style={{
              background: "#0a0714", border: "1px solid #1e1b2e",
              borderRadius: "10px", padding: "16px", marginBottom: "16px"
            }}>
              <div style={{
                fontSize: "10px", color: "#475569", textTransform: "uppercase",
                letterSpacing: "0.12em", marginBottom: "12px",
                display: "flex", justifyContent: "space-between"
              }}>
                <span>All Digits · {window}-tick window</span>
                <span>
                  <span style={{ color: "#a78bfa" }}>■ matches pick</span>
                  {"  "}
                  <span style={{ color: "#f472b6" }}>■ differs pick</span>
                </span>
              </div>
              <DigitGrid
                freq={freq} total={total}
                windowFreq={windowFreq} windowTotal={windowTotal}
                streaks={streaks}
                matchPick={top3Match[0]?.digit}
                differsPick={top3Differs[0]?.digit}
              />
            </div>

            {/* ── tick tape ── */}
            <div style={{
              background: "#0a0714", border: "1px solid #1e1b2e",
              borderRadius: "10px", padding: "14px", marginBottom: "16px"
            }}>
              <div style={{ fontSize: "10px", color: "#334155", textTransform: "uppercase", letterSpacing: "0.12em", marginBottom: "10px" }}>
                Last 30 ticks
                {lastPrice && <span style={{ float: "right", fontFamily: "monospace", color: "#1e293b" }}>{lastPrice.toFixed(5)}</span>}
              </div>
              <TickTape history={history} />
            </div>

            {/* ── stats bar ── */}
            <div style={{
              display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: "8px",
              marginBottom: "16px"
            }}>
              {[
                { label: "Total ticks", val: total },
                { label: "Hot digit", val: matchRanked[0]?.digit, sub: `${matchRanked[0]?.globalPct.toFixed(1)}%` },
                { label: "Cold digit", val: differsRanked[0]?.digit, sub: `${differsRanked[0]?.globalPct.toFixed(1)}%` },
                { label: "Longest drought", val: Object.values(streaks).reduce((a,b)=>Math.max(a,b),0), sub: "ticks" },
              ].map(({ label, val, sub }) => (
                <div key={label} style={{
                  background: "#0a0714", border: "1px solid #1e1b2e",
                  borderRadius: "8px", padding: "10px 12px", textAlign: "center"
                }}>
                  <div style={{ fontSize: "9px", color: "#334155", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: "3px" }}>{label}</div>
                  <div style={{ fontSize: "20px", fontFamily: "monospace", fontWeight: "800", color: "#94a3b8" }}>{val ?? "-"}</div>
                  {sub && <div style={{ fontSize: "9px", color: "#334155", marginTop: "1px" }}>{sub}</div>}
                </div>
              ))}
            </div>
          </>
        )}

        {/* disclaimer */}
        <div style={{ fontSize: "9px", color: "#1e1b2e", textAlign: "center", lineHeight: 1.7 }}>
          Statistical signals only. Deriv synthetic indices use a verified RNG — no outcome can be predicted with certainty.<br />
          Always trade within your risk limits.
        </div>
      </div>

      <style>{`
        @keyframes fadein { from { opacity: 0; transform: translateY(4px); } to { opacity: 1; transform: none; } }
        * { box-sizing: border-box; }
        select, input, button { font-family: inherit; }
        select:focus, input:focus { outline: 1px solid #7c3aed; }
        select option { background: #0d0a1a; }
      `}</style>
    </div>
  );
}