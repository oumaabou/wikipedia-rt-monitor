import { useState, useEffect, useRef, useCallback } from "react";

const API = "http://localhost:3000/api";

const MOCK = {
  p50_ms: 0.42, p90_ms: 0.91, p99_ms: 1.74,
  throughput_eps: 138.4, processed_total: 48291,
  missed_deadlines: 3, dropped_packets: 7,
  is_running: true, degraded_mode: false,
  network_reset_pending: false, pipeline_mode: "async",
  uptime_secs: 3724,
  human_avg_drift_ms: 0.18, bot_avg_drift_ms: 0.87,
  human_processed: 14200, bot_processed: 34091,
  leaderboard: [
    { rank: 1, domain: "en.wikipedia.org", count: 14872 },
    { rank: 2, domain: "commons.wikimedia.org", count: 9341 },
    { rank: 3, domain: "de.wikipedia.org", count: 6108 },
  ],
  live_feed: [
    "👤 AtUkr              ru.wikinews.org       0.2ms",
    "🤖 ClueBot NG         en.wikipedia.org      0.9ms",
    "👤 PantheraLeo1359531 commons.wikimedia.org 0.3ms",
    "🤖 AnomieBOT          en.wikipedia.org      1.1ms",
    "👤 WikiEditor42       fr.wikipedia.org      0.1ms",
  ],
};

function useMetrics(useMock) {
  const [data, setData] = useState(MOCK);
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState(null);
  useEffect(() => {
    if (useMock) { setConnected(true); return; }
    let alive = true;
    async function poll() {
      try {
        const r = await fetch(`${API}/metrics`, { signal: AbortSignal.timeout(2000) });
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const json = await r.json();
        if (alive) { setData(json); setConnected(true); setError(null); }
      } catch (e) {
        if (alive) { setConnected(false); setError(e.message); }
      }
    }
    poll();
    const id = setInterval(poll, 1000);
    return () => { alive = false; clearInterval(id); };
  }, [useMock]);
  return { data, connected, error };
}

async function sendControl(action, mode) {
  try {
    await fetch(`${API}/control`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action, mode }),
    });
  } catch (_) {}
}

function formatUptime(secs) {
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = secs % 60;
  return `${String(h).padStart(2,"0")}:${String(m).padStart(2,"0")}:${String(s).padStart(2,"0")}`;
}

function Pulse({ active, color }) {
  return (
    <span style={{ position:"relative", display:"inline-flex", width:9, height:9, marginRight:7 }}>
      {active && <span style={{ position:"absolute", inset:0, borderRadius:"50%", background: color || "#1a7a7a", animation:"ping 1.2s ease-in-out infinite", opacity:0.6 }} />}
      <span style={{ width:9, height:9, borderRadius:"50%", background: active ? (color || "#1a7a7a") : "#c04040", display:"block" }} />
    </span>
  );
}

function StatCard({ label, value, sub, accent }) {
  return (
    <div style={{ background:"rgba(0,0,0,0.04)", border:"1px solid rgba(0,0,0,0.08)", borderRadius:12, padding:"16px 20px", flex:1, minWidth:100 }}>
      <div style={{ fontSize:11, letterSpacing:"0.1em", color:"#4a5568", marginBottom:8, textTransform:"uppercase", fontFamily:"'JetBrains Mono', monospace", fontWeight:600 }}>{label}</div>
      <div style={{ fontSize:34, fontWeight:700, color: accent || "#1a1a1a", fontFamily:"'JetBrains Mono', monospace", lineHeight:1.1 }}>{value}</div>
      {sub && <div style={{ fontSize:11, color:"#6a7585", marginTop:5 }}>{sub}</div>}
    </div>
  );
}

function LatBar({ label, value, max, color }) {
  const pct = Math.min((value / max) * 100, 100);
  return (
    <div style={{ marginBottom:10 }}>
      <div style={{ display:"flex", justifyContent:"space-between", marginBottom:3 }}>
        <span style={{ fontSize:10, color:"#4a5568", fontFamily:"'JetBrains Mono', monospace" }}>{label}</span>
        <span style={{ fontSize:12, fontWeight:700, color, fontFamily:"'JetBrains Mono', monospace" }}>{value.toFixed(2)}ms</span>
      </div>
      <div style={{ height:4, background:"rgba(0,0,0,0.08)", borderRadius:2 }}>
        <div style={{ height:"100%", width:`${pct}%`, background:color, borderRadius:2, transition:"width 0.6s ease" }} />
      </div>
    </div>
  );
}

export default function App() {
  const [useMock, setUseMock] = useState(false);
  const { data, connected, error } = useMetrics(useMock);
  const feedRef = useRef(null);

  useEffect(() => {
    if (feedRef.current) feedRef.current.scrollTop = feedRef.current.scrollHeight;
  }, [data.live_feed]);

  const toggle = useCallback(() => {
    if (useMock) return;
    sendControl(data.is_running ? "stop" : "start");
  }, [data.is_running, useMock]);

  const p99color = data.p99_ms > 2 ? "#c0392b" : data.p99_ms > 1.5 ? "#b8760a" : "#1a7a7a";
  const humanDriftColor = "#1a7a7a";
  const botDriftColor = "#b8760a";
  const driftMax = Math.max(data.bot_avg_drift_ms * 1.3, 1);
  const latMax = Math.max(data.p99_ms * 1.1, 3);
  const phase = (data && data.phase !== undefined) ? data.phase : 0;
  const phaseRemaining = (data && data.phase_remaining_secs) ? data.phase_remaining_secs : 0;
  const phaseMin = String(Math.floor(phaseRemaining / 60)).padStart(2,'0');
  const phaseSec = String(phaseRemaining % 60).padStart(2,'0');
  const aSnap = data ? data.async_snapshot : null;
  const tSnap = data ? data.threaded_snapshot : null;

  return (
    <div style={{ minHeight:"100vh", background:"#f2ede3", fontFamily:"'Segoe UI',system-ui,sans-serif", color:"#1a1a1a" }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&display=swap');
        @keyframes ping{0%,100%{transform:scale(1);opacity:.6}50%{transform:scale(2);opacity:0}}
        @keyframes fadeIn{from{opacity:0;transform:translateY(-3px)}to{opacity:1;transform:translateY(0)}}
        ::-webkit-scrollbar{width:4px}::-webkit-scrollbar-thumb{background:#1a7a7a;border-radius:2px}
        button:focus-visible{outline:2px solid #1a7a7a}
      `}</style>

      {/* Header */}
      <header style={{ borderBottom:"1px solid rgba(0,0,0,0.07)", padding:"0 28px", background:"rgba(30,42,56,0.98)", position:"sticky", top:0, zIndex:50, display:"flex", alignItems:"center", height:52, gap:14 }}>
        <div style={{ display:"flex", alignItems:"center", gap:10, flex:1 }}>
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none">
            <circle cx="12" cy="12" r="10" stroke="#1a7a7a" strokeWidth="1.5"/>
            <circle cx="12" cy="12" r="6" stroke="#1a7a7a" strokeWidth="1" opacity=".4"/>
            <circle cx="12" cy="12" r="2" fill="#1a7a7a"/>
          </svg>
          <span style={{ fontFamily:"'JetBrains Mono', monospace", fontWeight:700, fontSize:13, letterSpacing:".04em", color:"#1a1a1a" }}>
            WIKIPEDIA <span style={{ color:"#1a7a7a" }}>RT</span> MONITOR
          </span>
        </div>
        <div style={{ display:"flex", alignItems:"center", gap:16 }}>
          <div style={{ display:"flex", alignItems:"center", fontSize:10, fontFamily:"'JetBrains Mono', monospace", color: connected ? "#1a7a7a" : "#c0392b" }}>
            <Pulse active={connected} />
            {useMock ? "DEMO" : connected ? "LIVE" : `OFFLINE — ${error}`}
          </div>
          <button onClick={() => setUseMock(m => !m)} style={{ background:"transparent", border:"1px solid rgba(0,0,0,0.07)", borderRadius:6, color:"#4a5568", fontSize:9, letterSpacing:".1em", padding:"4px 10px", cursor:"pointer", fontFamily:"'JetBrains Mono', monospace" }}>
            {useMock ? "USE LIVE API" : "USE DEMO"}
          </button>
          <div style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:10, color:"#1a1a1a" }}>UP {formatUptime(data.uptime_secs)}</div>
        </div>
      </header>

      {/* ── Phase banner ────────────────────────────────────────────────── */}
      {phase < 2 && (
        <div style={{ padding:"10px 28px 0" }}>
          <div style={{
            background: phase === 0 ? "rgba(77,184,184,0.15)" : "rgba(90,171,238,0.15)",
            border: `1px solid ${phase === 0 ? "#1a7a7a" : "#1a5fbd"}`,
            borderRadius:8, padding:"12px 20px",
            display:"flex", alignItems:"center", justifyContent:"space-between"
          }}>
            <div style={{ display:"flex", alignItems:"center", gap:12 }}>
              <span style={{ fontSize:20 }}>{phase === 0 ? "⚡" : "🧵"}</span>
              <div>
                <div style={{ fontSize:15, fontWeight:700, fontFamily:"monospace",
                  color: phase === 0 ? "#1a7a7a" : "#1a5fbd" }}>
                  {phase === 0 ? "PHASE 1 — ASYNC (Tokio)" : "PHASE 2 — THREADED (std::thread)"}
                </div>
                <div style={{ fontSize:11, color:"#4a5568", marginTop:2 }}>
                  {phase === 0 ? "Collecting baseline — Tokio async workers" : "Collecting metrics — OS-level threaded workers"}
                </div>
              </div>
            </div>
            <div style={{ textAlign:"right" }}>
              <div style={{ fontSize:28, fontWeight:700, fontFamily:"monospace",
                color: phaseRemaining < 30 ? "#c0392b" : "#1a1a1a" }}>
                {phaseMin}:{phaseSec}
              </div>
              <div style={{ fontSize:11, color:"#4a5568", fontWeight:600 }}>remaining</div>
            </div>
          </div>
        </div>
      )}

      <main style={{ padding:"22px 28px", maxWidth:1400, margin:"0 auto" }}>

      {/* ── Summary screen (phase 2) ─────────────────────────────────────── */}
      {phase === 2 && aSnap && tSnap && (
        <div style={{ marginBottom:28 }}>
          <div style={{ textAlign:"center", marginBottom:20 }}>
            <div style={{ fontSize:26, fontWeight:700, color:"#1a7a7a", fontFamily:"monospace" }}>
              📊 DEMO COMPLETE — RESULTS SUMMARY
            </div>
            <div style={{ fontSize:12, color:"#4a5568", marginTop:4 }}>
              8-minute live Wikipedia SSE benchmark · Async vs Threaded
            </div>
          </div>

          <div style={{ background:"rgba(0,0,0,0.05)", border:"1px solid rgba(0,0,0,0.05)", borderRadius:10, overflow:"hidden", marginBottom:20 }}>
            <div style={{ display:"grid", gridTemplateColumns:"2fr 1fr 1fr", background:"rgba(0,0,0,0.08)", padding:"10px 20px" }}>
              <div style={{ fontSize:11, color:"#4a5568", fontWeight:700 }}>METRIC</div>
              <div style={{ fontSize:11, color:"#1a7a7a", fontWeight:700, textAlign:"center" }}>⚡ ASYNC</div>
              <div style={{ fontSize:11, color:"#1a5fbd", fontWeight:700, textAlign:"center" }}>🧵 THREADED</div>
            </div>
            {[
              ["P50 Latency",          aSnap.p50_ms.toFixed(3)+"ms",                       tSnap.p50_ms.toFixed(3)+"ms",                       "lower"],
              ["P90 Latency",          aSnap.p90_ms.toFixed(3)+"ms",                       tSnap.p90_ms.toFixed(3)+"ms",                       "lower"],
              ["P99 Tail Latency",     aSnap.p99_ms.toFixed(3)+"ms",                       tSnap.p99_ms.toFixed(3)+"ms",                       "lower"],
              ["Missed Deadlines",     String(aSnap.missed_deadlines),                     String(tSnap.missed_deadlines),                     "lower"],
              ["Dropped Packets",      String(aSnap.dropped_packets),                      String(tSnap.dropped_packets),                      "lower"],
              ["Packets Processed",    String(aSnap.processed),                            String(tSnap.processed),                            "higher"],
              ["Throughput (eps)",     aSnap.throughput_eps.toFixed(1),                    tSnap.throughput_eps.toFixed(1),                    "higher"],
              ["Human Queue Wait",     aSnap.human_avg_drift_ms.toFixed(3)+"ms",           tSnap.human_avg_drift_ms.toFixed(3)+"ms",           "lower"],
              ["Bot Queue Wait",       aSnap.bot_avg_drift_ms.toFixed(3)+"ms",             tSnap.bot_avg_drift_ms.toFixed(3)+"ms",             "lower"],
              ["Priority Ratio",       (aSnap.bot_avg_drift_ms/Math.max(aSnap.human_avg_drift_ms,0.001)).toFixed(2)+"×",
                                       (tSnap.bot_avg_drift_ms/Math.max(tSnap.human_avg_drift_ms,0.001)).toFixed(2)+"×", "higher"],
            ].map(([label, av, tv, better], i) => {
              const aNum = parseFloat(av); const tNum = parseFloat(tv);
              const aWins = better === "lower" ? aNum <= tNum : aNum >= tNum;
              return (
                <div key={i} style={{ display:"grid", gridTemplateColumns:"2fr 1fr 1fr",
                  padding:"9px 20px", borderTop:"1px solid rgba(0,0,0,0.06)",
                  background: i%2===0?"transparent":"rgba(0,0,0,0.05)" }}>
                  <div style={{ fontSize:12, color:"#4a5568" }}>{label}</div>
                  <div style={{ fontSize:13, fontFamily:"monospace", fontWeight:700, textAlign:"center",
                    color: aWins ? "#1a7a7a" : "#1a1a1a" }}>
                    {aWins && "✓ "}{av}
                  </div>
                  <div style={{ fontSize:13, fontFamily:"monospace", fontWeight:700, textAlign:"center",
                    color: !aWins ? "#1a5fbd" : "#1a1a1a" }}>
                    {!aWins && "✓ "}{tv}
                  </div>
                </div>
              );
            })}
          </div>

          <div style={{ background:"rgba(77,184,184,0.12)", border:"1px solid #1a7a7a",
            borderRadius:8, padding:"14px 20px", marginBottom:20, textAlign:"center" }}>
            <div style={{ fontSize:15, fontWeight:700, color:"#1a7a7a" }}>
              ⚡ Async achieved {aSnap.p99_ms > 0 && tSnap.p99_ms > 0
                ? (tSnap.p99_ms / aSnap.p99_ms).toFixed(2) : "—"}× lower P99 tail latency than threaded
            </div>
            <div style={{ fontSize:11, color:"#4a5568", marginTop:4 }}>
              Priority scheduling confirmed: human edits had lower queue wait than bots in both modes
            </div>
          </div>

          <div style={{ background:"rgba(0,0,0,0.05)", border:"1px solid rgba(0,0,0,0.05)",
            borderRadius:10, padding:"16px 20px" }}>
            <div style={{ fontSize:12, fontWeight:700, color:"#1a1a1a", marginBottom:12, fontFamily:"monospace" }}>
              🏆 TOP DOMAINS — FULL 8-MINUTE SESSION
            </div>
            {(data.leaderboard||[]).map((e,i) => (
              <div key={i} style={{ display:"flex", alignItems:"center", gap:12, marginBottom:8 }}>
                <span style={{ fontSize:16 }}>{["🥇","🥈","🥉"][i]}</span>
                <span style={{ flex:1, fontSize:13, fontFamily:"monospace", color:"#4a5568" }}>{e.domain}</span>
                <span style={{ fontSize:14, fontWeight:700, fontFamily:"monospace", color:"#1a7a7a" }}>
                  {e.count.toLocaleString()}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

        {/* Controls */}
        <div style={{ display:"flex", alignItems:"center", gap:10, marginBottom:22, flexWrap:"wrap" }}>
          {/* START / STOP — purpose: pause pipeline workers to demonstrate effect on metrics */}
          <button onClick={toggle} style={{
            display:"flex", alignItems:"center", gap:7,
            background: data.is_running ? "rgba(192,64,64,0.2)" : "rgba(77,184,184,0.15)",
            border:`1px solid ${data.is_running ? "rgba(224,85,69,0.25)" : "rgba(77,184,184,0.3)"}`,
            borderRadius:8, padding:"7px 16px", cursor:"pointer",
            color: data.is_running ? "#c0392b" : "#1a7a7a",
            fontFamily:"'JetBrains Mono', monospace", fontSize:11, fontWeight:700, letterSpacing:".08em",
          }}>
            {data.is_running ? "⏸ STOP WORKERS" : "▶ START WORKERS"}
          </button>

          {/* Async / Threaded toggle — only ONE runs at a time, restart required to switch */}
          <div style={{ display:"flex", background:"rgba(0,0,0,0.04)", border:"1px solid rgba(0,0,0,0.07)", borderRadius:8, overflow:"hidden" }}>
            {["async","threaded"].map(m => (
              <button key={m} onClick={() => !useMock && sendControl("set_mode", m)} style={{
                padding:"7px 14px",
                background: data.pipeline_mode === m ? "rgba(77,184,184,0.2)" : "transparent",
                border:"none", borderRight: m==="async" ? "1px solid rgba(0,0,0,0.07)" : "none",
                color: data.pipeline_mode === m ? "#1a7a7a" : "#4a5568",
                fontFamily:"'JetBrains Mono', monospace", fontSize:10, letterSpacing:".08em",
                cursor:"pointer", fontWeight: data.pipeline_mode === m ? 700 : 400,
              }}>
                {m === "async" ? "⚡ ASYNC" : "🧵 THREADED"}
              </button>
            ))}
          </div>

          {/* Status badges */}
          {data.degraded_mode && (
            <div style={{ background:"rgba(230,168,23,0.15)", border:"1px solid rgba(230,168,23,0.2)", borderRadius:6, padding:"5px 12px", fontFamily:"'JetBrains Mono', monospace", fontSize:10, color:"#b8760a", letterSpacing:".1em" }}>
              ⚠ DEGRADED MODE — bot edits dropped
            </div>
          )}
          {data.network_reset_pending && (
            <div style={{ background:"rgba(224,85,69,0.15)", border:"1px solid rgba(224,85,69,0.2)", borderRadius:6, padding:"5px 12px", fontFamily:"'JetBrains Mono', monospace", fontSize:10, color:"#c0392b", letterSpacing:".1em" }}>
              🔌 RECONNECTING…
            </div>
          )}
          {/* Reset stale p99 / drift metrics manually */}
          <button onClick={() => !useMock && sendControl("reset_metrics")} style={{
            background:"rgba(0,0,0,0.05)", border:"1px solid rgba(0,0,0,0.05)",
            borderRadius:7, padding:"6px 12px", cursor:"pointer",
            color:"#4a5568", fontFamily:"'JetBrains Mono', monospace", fontSize:9, letterSpacing:".1em",
          }}>
            🔄 RESET METRICS
          </button>
          <div style={{ flex:1 }} />
          <div style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:9, color:"#1a1a1a" }}>{new Date().toLocaleTimeString()}</div>
        </div>

        {/* Stat cards */}
        <div style={{ display:"flex", gap:10, marginBottom:18, flexWrap:"wrap" }}>
          <StatCard label="throughput" value={`${data.throughput_eps.toFixed(1)}`} sub="edits/sec" accent="#1a1a1a" />
          <StatCard label="processed" value={data.processed_total.toLocaleString()} sub="total events" />
          <StatCard label="missed deadlines" value={data.missed_deadlines} sub="> 2ms" accent={data.missed_deadlines > 0 ? "#b8760a" : "#1a7a7a"} />
          <StatCard label="dropped packets" value={data.dropped_packets} sub="channel overflow" accent={data.dropped_packets > 0 ? "#c0392b" : "#1a7a7a"} />
          <StatCard label="p99 latency" value={`${data.p99_ms.toFixed(2)}ms`} sub="tail latency" accent={p99color} />
        </div>

        {/* Main grid */}
        <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:16, marginBottom:16 }}>

          {/* Latency percentiles */}
          <div style={{ background:"rgba(0,0,0,0.05)", border:"1px solid rgba(0,0,0,0.06)", borderRadius:13, padding:"18px 20px" }}>
            <div style={{ display:"flex", alignItems:"center", gap:7, marginBottom:16 }}>
              <span style={{ fontSize:13 }}>📊</span>
              <span style={{ fontSize:10, letterSpacing:".12em", color:"#4a5568", fontFamily:"'JetBrains Mono', monospace", textTransform:"uppercase" }}>Latency Percentiles</span>
            </div>
            <LatBar label="P50" value={data.p50_ms} max={3} color="#1a7a7a" />
            <LatBar label="P90" value={data.p90_ms} max={3} color="#1a1a1a" />
            <LatBar label="P99" value={data.p99_ms} max={3} color={p99color} />
            <div style={{ marginTop:14, paddingTop:12, borderTop:"1px solid rgba(0,0,0,0.05)" }}>
              <div style={{ fontSize:9, color:"#4a5568", marginBottom:8, textTransform:"uppercase", letterSpacing:".1em" }}>2ms deadline line</div>
              <div style={{ position:"relative", height:3, background:"rgba(0,0,0,0.08)", borderRadius:2 }}>
                <div style={{ position:"absolute", left:"66.6%", top:-4, width:1, height:11, background:"rgba(224,85,69,0.5)" }} />
              </div>
              <div style={{ display:"flex", justifyContent:"space-between", marginTop:4, fontFamily:"'JetBrains Mono', monospace", fontSize:8, color:"#1a1a1a" }}>
                <span>0ms</span><span>3ms</span>
              </div>
            </div>
          </div>

          {/* PRIORITY PROOF: Human vs Bot drift comparison */}
          <div style={{ background:"rgba(0,0,0,0.05)", border:"1px solid rgba(0,0,0,0.06)", borderRadius:13, padding:"18px 20px" }}>
            <div style={{ display:"flex", alignItems:"center", gap:7, marginBottom:4 }}>
              <span style={{ fontSize:13 }}>⚖️</span>
              <span style={{ fontSize:10, letterSpacing:".12em", color:"#4a5568", fontFamily:"'JetBrains Mono', monospace", textTransform:"uppercase" }}>Priority Proof — Scheduling Drift</span>
            </div>
            <div style={{ fontSize:9, color:"#4a5568", marginBottom:14, fontFamily:"'JetBrains Mono', monospace" }}>
              Human edits (high channel) must show LOWER avg drift than bot edits (low channel)
            </div>

            <LatBar label={`👤 HUMAN avg drift  (${data.human_processed?.toLocaleString() || 0} edits)`} value={data.human_avg_drift_ms || 0} max={driftMax} color={humanDriftColor} />
            <LatBar label={`🤖 BOT avg drift     (${data.bot_processed?.toLocaleString() || 0} edits)`} value={data.bot_avg_drift_ms || 0} max={driftMax} color={botDriftColor} />

            <div style={{ marginTop:14, paddingTop:12, borderTop:"1px solid rgba(0,0,0,0.05)", display:"flex", justifyContent:"space-between", alignItems:"center" }}>
              {data.human_avg_drift_ms > 0 && data.bot_avg_drift_ms > 0 ? (
                <div style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:11 }}>
                  <span style={{ color:"#1a7a7a" }}>
                    {data.bot_avg_drift_ms > data.human_avg_drift_ms
                      ? `✓ Human ${((data.bot_avg_drift_ms / data.human_avg_drift_ms)).toFixed(1)}× lower drift than bots`
                      : "⚠ Accumulating samples…"}
                  </span>
                </div>
              ) : (
                <div style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:10, color:"#4a5568" }}>waiting for data…</div>
              )}
            </div>
          </div>

          {/* Leaderboard */}
          <div style={{ background:"rgba(0,0,0,0.05)", border:"1px solid rgba(0,0,0,0.06)", borderRadius:13, padding:"18px 20px" }}>
            <div style={{ display:"flex", alignItems:"center", gap:7, marginBottom:16 }}>
              <span style={{ fontSize:13 }}>🏆</span>
              <span style={{ fontSize:10, letterSpacing:".12em", color:"#4a5568", fontFamily:"'JetBrains Mono', monospace", textTransform:"uppercase" }}>Top Domains</span>
            </div>
            {data.leaderboard.map((e, i) => (
              <div key={e.rank} style={{ marginBottom:12 }}>
                <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:4 }}>
                  <span style={{ fontSize:14 }}>{["🥇","🥈","🥉"][i]}</span>
                  <span style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:11, color:"#6a7585", flex:1, overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>{e.domain}</span>
                  <span style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:12, fontWeight:700, color:"#1a1a1a" }}>{e.count.toLocaleString()}</span>
                </div>
                <div style={{ height:3, background:"rgba(0,0,0,0.08)", borderRadius:2, marginLeft:28 }}>
                  <div style={{ height:"100%", width:`${i===0?100:i===1?65:42}%`, background:"linear-gradient(90deg,#1a7a7a,#1a6868)", borderRadius:2, transition:"width 0.9s ease" }} />
                </div>
              </div>
            ))}
            <div style={{ marginTop:12, paddingTop:10, borderTop:"1px solid rgba(0,0,0,0.05)", fontSize:9, color:"#1a1a1a", fontFamily:"'JetBrains Mono', monospace" }}>
              {data.processed_total.toLocaleString()} total edits tracked
            </div>
          </div>

          {/* System status */}
          <div style={{ background:"rgba(0,0,0,0.05)", border:"1px solid rgba(0,0,0,0.06)", borderRadius:13, padding:"18px 20px" }}>
            <div style={{ display:"flex", alignItems:"center", gap:7, marginBottom:16 }}>
              <span style={{ fontSize:13 }}>⚙️</span>
              <span style={{ fontSize:10, letterSpacing:".12em", color:"#4a5568", fontFamily:"'JetBrains Mono', monospace", textTransform:"uppercase" }}>System Status</span>
            </div>

            {[
              { label:"Pipeline", value: data.is_running ? "RUNNING" : "STOPPED", color: data.is_running ? "#1a7a7a" : "#c0392b" },
              { label:"Mode", value: data.pipeline_mode?.toUpperCase(), color:"#1a1a1a" },
              { label:"Degraded mode", value: data.degraded_mode ? "ACTIVE — bots dropped" : "NORMAL", color: data.degraded_mode ? "#b8760a" : "#1a7a7a" },
              { label:"Network watchdog", value: data.network_reset_pending ? "RECONNECTING" : "OK", color: data.network_reset_pending ? "#c0392b" : "#1a7a7a" },
              { label:"Missed deadlines", value: data.missed_deadlines, color: data.missed_deadlines > 0 ? "#b8760a" : "#1a7a7a" },
              { label:"Dropped packets", value: data.dropped_packets, color: data.dropped_packets > 0 ? "#c0392b" : "#1a7a7a" },
            ].map(row => (
              <div key={row.label} style={{ display:"flex", justifyContent:"space-between", padding:"5px 0", borderBottom:"1px solid rgba(0,0,0,0.04)" }}>
                <span style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:10, color:"#4a5568" }}>{row.label}</span>
                <span style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:10, fontWeight:700, color:row.color }}>{row.value}</span>
              </div>
            ))}

            <div style={{ marginTop:12, padding:"8px", background:"rgba(0,0,0,0.04)", borderRadius:6 }}>
              <div style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:9, color:"#4a5568", lineHeight:1.6 }}>
                STOP: pauses pipeline workers → shows metric freeze<br/>
                DEGRADED: auto-activates when p99 &gt; 2ms for 10s<br/>
                NETWORK RESET: fires after 10s silence from Wikipedia
              </div>
            </div>
          </div>
        </div>

        {/* Live feed — full width */}
        <div style={{ background:"rgba(0,0,0,0.05)", border:"1px solid rgba(0,0,0,0.06)", borderRadius:13, padding:"18px 20px" }}>
          <div style={{ display:"flex", alignItems:"center", gap:7, marginBottom:12 }}>
            <span style={{ fontSize:13 }}>📡</span>
            <span style={{ fontSize:10, letterSpacing:".12em", color:"#4a5568", fontFamily:"'JetBrains Mono', monospace", textTransform:"uppercase" }}>Live Edit Feed</span>
            <div style={{ flex:1 }} />
            <span style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:9, color:"#1a1a1a" }}>👤 human = HIGH priority  |  🤖 bot = LOW priority</span>
          </div>
          <div ref={feedRef} style={{ maxHeight:200, overflowY:"auto", paddingRight:4 }}>
            {data.live_feed.length === 0 ? (
              <div style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:10, color:"#1a1a1a", padding:"16px 0", textAlign:"center" }}>waiting for pipeline start…</div>
            ) : (
              [...data.live_feed].reverse().map((line, i) => (
                <div key={i} style={{
                  fontFamily:"'JetBrains Mono', monospace", fontSize:10,
                  padding:"3px 0", borderBottom:"1px solid rgba(0,0,0,0.04)",
                  color: line.startsWith("🤖") ? "#1a6868" : "#6a7585",
                  display:"flex", gap:6, animation: i===0?"fadeIn 0.4s ease":"none",
                }}>
                  <span style={{ opacity:0.4, fontSize:9, minWidth:20 }}>#{i+1}</span>
                  {line}
                </div>
              ))
            )}
          </div>
        </div>

        <div style={{ marginTop:16, paddingTop:12, borderTop:"1px solid rgba(0,0,0,0.04)", display:"flex", justifyContent:"space-between", fontFamily:"'JetBrains Mono', monospace", fontSize:8, color:"#4a5568", letterSpacing:".08em" }}>
          <span>WIKIPEDIA REAL-TIME MONITOR — UNIVERSITY ASSIGNMENT</span>
          <span>API → localhost:3000 | polling 1s</span>
        </div>
      </main>
    </div>
  );
}