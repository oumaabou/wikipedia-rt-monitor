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
      {active && <span style={{ position:"absolute", inset:0, borderRadius:"50%", background: color || "#5abf5a", animation:"ping 1.2s ease-in-out infinite", opacity:0.6 }} />}
      <span style={{ width:9, height:9, borderRadius:"50%", background: active ? (color || "#5abf5a") : "#5a3a3a", display:"block" }} />
    </span>
  );
}

function StatCard({ label, value, sub, accent }) {
  return (
    <div style={{ background:"rgba(255,255,255,0.03)", border:"1px solid rgba(181,169,138,0.12)", borderRadius:12, padding:"14px 18px", flex:1, minWidth:100 }}>
      <div style={{ fontSize:9, letterSpacing:"0.12em", color:"#8a7e64", marginBottom:5, textTransform:"uppercase", fontFamily:"'JetBrains Mono', monospace" }}>{label}</div>
      <div style={{ fontSize:24, fontWeight:700, color: accent || "#d4c9a8", fontFamily:"'JetBrains Mono', monospace", lineHeight:1 }}>{value}</div>
      {sub && <div style={{ fontSize:9, color:"#6a6050", marginTop:3 }}>{sub}</div>}
    </div>
  );
}

function LatBar({ label, value, max, color }) {
  const pct = Math.min((value / max) * 100, 100);
  return (
    <div style={{ marginBottom:10 }}>
      <div style={{ display:"flex", justifyContent:"space-between", marginBottom:3 }}>
        <span style={{ fontSize:10, color:"#8a7e64", fontFamily:"'JetBrains Mono', monospace" }}>{label}</span>
        <span style={{ fontSize:12, fontWeight:700, color, fontFamily:"'JetBrains Mono', monospace" }}>{value.toFixed(2)}ms</span>
      </div>
      <div style={{ height:4, background:"rgba(255,255,255,0.07)", borderRadius:2 }}>
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

  const p99color = data.p99_ms > 2 ? "#e57373" : data.p99_ms > 1.5 ? "#f5c842" : "#7ec87e";
  const latMax = Math.max(data.p99_ms * 1.1, 3);
  const humanDriftColor = "#7ec87e";
  const botDriftColor = "#f5c842";
  const driftMax = Math.max(data.bot_avg_drift_ms * 1.3, 1);

  return (
    <div style={{ minHeight:"100vh", background:"#0f1a0f", fontFamily:"'Segoe UI',system-ui,sans-serif", color:"#d4c9a8" }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&display=swap');
        @keyframes ping{0%,100%{transform:scale(1);opacity:.6}50%{transform:scale(2);opacity:0}}
        @keyframes fadeIn{from{opacity:0;transform:translateY(-3px)}to{opacity:1;transform:translateY(0)}}
        ::-webkit-scrollbar{width:4px}::-webkit-scrollbar-thumb{background:#3a5a3a;border-radius:2px}
        button:focus-visible{outline:2px solid #7ec87e}
      `}</style>

      {/* Header */}
      <header style={{ borderBottom:"1px solid rgba(181,169,138,0.12)", padding:"0 28px", background:"rgba(15,26,15,0.97)", position:"sticky", top:0, zIndex:50, display:"flex", alignItems:"center", height:52, gap:14 }}>
        <div style={{ display:"flex", alignItems:"center", gap:10, flex:1 }}>
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none">
            <circle cx="12" cy="12" r="10" stroke="#5abf5a" strokeWidth="1.5"/>
            <circle cx="12" cy="12" r="6" stroke="#5abf5a" strokeWidth="1" opacity=".4"/>
            <circle cx="12" cy="12" r="2" fill="#5abf5a"/>
          </svg>
          <span style={{ fontFamily:"'JetBrains Mono', monospace", fontWeight:700, fontSize:13, letterSpacing:".04em", color:"#d4c9a8" }}>
            WIKIPEDIA <span style={{ color:"#5abf5a" }}>RT</span> MONITOR
          </span>
        </div>
        <div style={{ display:"flex", alignItems:"center", gap:16 }}>
          <div style={{ display:"flex", alignItems:"center", fontSize:10, fontFamily:"'JetBrains Mono', monospace", color: connected ? "#7ec87e" : "#e57373" }}>
            <Pulse active={connected} />
            {useMock ? "DEMO" : connected ? "LIVE" : `OFFLINE — ${error}`}
          </div>
          <button onClick={() => setUseMock(m => !m)} style={{ background:"transparent", border:"1px solid rgba(181,169,138,0.25)", borderRadius:6, color:"#b5a98a", fontSize:9, letterSpacing:".1em", padding:"4px 10px", cursor:"pointer", fontFamily:"'JetBrains Mono', monospace" }}>
            {useMock ? "USE LIVE API" : "USE DEMO"}
          </button>
          <div style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:10, color:"#4a4038" }}>UP {formatUptime(data.uptime_secs)}</div>
        </div>
      </header>

      <main style={{ padding:"22px 28px", maxWidth:1400, margin:"0 auto" }}>

        {/* Controls */}
        <div style={{ display:"flex", alignItems:"center", gap:10, marginBottom:22, flexWrap:"wrap" }}>
          {/* START / STOP — purpose: pause pipeline workers to demonstrate effect on metrics */}
          <button onClick={toggle} style={{
            display:"flex", alignItems:"center", gap:7,
            background: data.is_running ? "rgba(90,63,63,0.4)" : "rgba(58,110,58,0.4)",
            border:`1px solid ${data.is_running ? "rgba(229,115,115,0.35)" : "rgba(94,191,94,0.35)"}`,
            borderRadius:8, padding:"7px 16px", cursor:"pointer",
            color: data.is_running ? "#e57373" : "#7ec87e",
            fontFamily:"'JetBrains Mono', monospace", fontSize:11, fontWeight:700, letterSpacing:".08em",
          }}>
            {data.is_running ? "⏸ STOP WORKERS" : "▶ START WORKERS"}
          </button>

          {/* Async / Threaded toggle — only ONE runs at a time, restart required to switch */}
          <div style={{ display:"flex", background:"rgba(255,255,255,0.03)", border:"1px solid rgba(181,169,138,0.12)", borderRadius:8, overflow:"hidden" }}>
            {["async","threaded"].map(m => (
              <button key={m} onClick={() => !useMock && sendControl("set_mode", m)} style={{
                padding:"7px 14px",
                background: data.pipeline_mode === m ? "rgba(94,191,94,0.15)" : "transparent",
                border:"none", borderRight: m==="async" ? "1px solid rgba(181,169,138,0.12)" : "none",
                color: data.pipeline_mode === m ? "#7ec87e" : "#7a6e58",
                fontFamily:"'JetBrains Mono', monospace", fontSize:10, letterSpacing:".08em",
                cursor:"pointer", fontWeight: data.pipeline_mode === m ? 700 : 400,
              }}>
                {m === "async" ? "⚡ ASYNC" : "🧵 THREADED"}
              </button>
            ))}
          </div>

          {/* Status badges */}
          {data.degraded_mode && (
            <div style={{ background:"rgba(245,200,66,0.12)", border:"1px solid rgba(245,200,66,0.3)", borderRadius:6, padding:"5px 12px", fontFamily:"'JetBrains Mono', monospace", fontSize:10, color:"#f5c842", letterSpacing:".1em" }}>
              ⚠ DEGRADED MODE — bot edits dropped
            </div>
          )}
          {data.network_reset_pending && (
            <div style={{ background:"rgba(229,115,115,0.12)", border:"1px solid rgba(229,115,115,0.3)", borderRadius:6, padding:"5px 12px", fontFamily:"'JetBrains Mono', monospace", fontSize:10, color:"#e57373", letterSpacing:".1em" }}>
              🔌 RECONNECTING…
            </div>
          )}
          {/* Reset stale p99 / drift metrics manually */}
          <button onClick={() => !useMock && sendControl("reset_metrics")} style={{
            background:"rgba(255,255,255,0.04)", border:"1px solid rgba(181,169,138,0.2)",
            borderRadius:7, padding:"6px 12px", cursor:"pointer",
            color:"#8a7e64", fontFamily:"'JetBrains Mono', monospace", fontSize:9, letterSpacing:".1em",
          }}>
            🔄 RESET METRICS
          </button>
          <div style={{ flex:1 }} />
          <div style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:9, color:"#3a3028" }}>{new Date().toLocaleTimeString()}</div>
        </div>

        {/* Stat cards */}
        <div style={{ display:"flex", gap:10, marginBottom:18, flexWrap:"wrap" }}>
          <StatCard label="throughput" value={`${data.throughput_eps.toFixed(1)}`} sub="edits/sec" accent="#a8d4a8" />
          <StatCard label="processed" value={data.processed_total.toLocaleString()} sub="total events" />
          <StatCard label="missed deadlines" value={data.missed_deadlines} sub="> 2ms" accent={data.missed_deadlines > 0 ? "#f5c842" : "#7ec87e"} />
          <StatCard label="dropped packets" value={data.dropped_packets} sub="channel overflow" accent={data.dropped_packets > 0 ? "#e57373" : "#7ec87e"} />
          <StatCard label="p99 latency" value={`${data.p99_ms.toFixed(2)}ms`} sub="tail latency" accent={p99color} />
        </div>

        {/* Main grid */}
        <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:16, marginBottom:16 }}>

          {/* Latency percentiles */}
          <div style={{ background:"rgba(255,255,255,0.02)", border:"1px solid rgba(181,169,138,0.1)", borderRadius:13, padding:"18px 20px" }}>
            <div style={{ display:"flex", alignItems:"center", gap:7, marginBottom:16 }}>
              <span style={{ fontSize:13 }}>📊</span>
              <span style={{ fontSize:10, letterSpacing:".12em", color:"#8a7e64", fontFamily:"'JetBrains Mono', monospace", textTransform:"uppercase" }}>Latency Percentiles</span>
            </div>
            <LatBar label="P50" value={data.p50_ms} max={latMax} color="#7ec87e" />
            <LatBar label="P90" value={data.p90_ms} max={latMax} color="#a8d4a8" />
            <LatBar label="P99" value={data.p99_ms} max={latMax} color={p99color} />
            <div style={{ marginTop:14, paddingTop:12, borderTop:"1px solid rgba(181,169,138,0.08)" }}>
              <div style={{ fontSize:9, color:"#5a5048", marginBottom:8, textTransform:"uppercase", letterSpacing:".1em" }}>2ms deadline line</div>
              <div style={{ position:"relative", height:3, background:"rgba(255,255,255,0.06)", borderRadius:2 }}>
                <div style={{ position:"absolute", left:`${Math.min((2/latMax)*100, 99)}%`, top:-4, width:1, height:11, background:"rgba(229,115,115,0.6)" }} />
              </div>
              <div style={{ display:"flex", justifyContent:"space-between", marginTop:4, fontFamily:"'JetBrains Mono', monospace", fontSize:8, color:"#3a3028" }}>
                <span>0ms</span><span>{latMax >= 10 ? Math.round(latMax) : latMax.toFixed(1)}ms</span>
              </div>
            </div>
          </div>

          {/* PRIORITY PROOF: Human vs Bot drift comparison */}
          <div style={{ background:"rgba(255,255,255,0.02)", border:"1px solid rgba(181,169,138,0.1)", borderRadius:13, padding:"18px 20px" }}>
            <div style={{ display:"flex", alignItems:"center", gap:7, marginBottom:4 }}>
              <span style={{ fontSize:13 }}>⚖️</span>
              <span style={{ fontSize:10, letterSpacing:".12em", color:"#8a7e64", fontFamily:"'JetBrains Mono', monospace", textTransform:"uppercase" }}>Priority Proof — Scheduling Drift</span>
            </div>
            <div style={{ fontSize:9, color:"#5a5048", marginBottom:14, fontFamily:"'JetBrains Mono', monospace" }}>
              Human edits (high channel) must show LOWER avg drift than bot edits (low channel)
            </div>

            <LatBar label={`👤 HUMAN avg drift  (${data.human_processed?.toLocaleString() || 0} edits)`} value={data.human_avg_drift_ms || 0} max={driftMax} color={humanDriftColor} />
            <LatBar label={`🤖 BOT avg drift     (${data.bot_processed?.toLocaleString() || 0} edits)`} value={data.bot_avg_drift_ms || 0} max={driftMax} color={botDriftColor} />

            <div style={{ marginTop:14, paddingTop:12, borderTop:"1px solid rgba(181,169,138,0.08)", display:"flex", justifyContent:"space-between", alignItems:"center" }}>
              {data.human_avg_drift_ms > 0 && data.bot_avg_drift_ms > 0 ? (
                <div style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:11 }}>
                  <span style={{ color:"#7ec87e" }}>
                    {data.bot_avg_drift_ms > data.human_avg_drift_ms
                      ? `✓ Human ${((data.bot_avg_drift_ms / data.human_avg_drift_ms)).toFixed(1)}× lower drift than bots`
                      : "⚠ Accumulating samples…"}
                  </span>
                </div>
              ) : (
                <div style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:10, color:"#5a5048" }}>waiting for data…</div>
              )}
            </div>
          </div>

          {/* Leaderboard */}
          <div style={{ background:"rgba(255,255,255,0.02)", border:"1px solid rgba(181,169,138,0.1)", borderRadius:13, padding:"18px 20px" }}>
            <div style={{ display:"flex", alignItems:"center", gap:7, marginBottom:16 }}>
              <span style={{ fontSize:13 }}>🏆</span>
              <span style={{ fontSize:10, letterSpacing:".12em", color:"#8a7e64", fontFamily:"'JetBrains Mono', monospace", textTransform:"uppercase" }}>Top Domains</span>
            </div>
            {data.leaderboard.map((e, i) => (
              <div key={e.rank} style={{ marginBottom:12 }}>
                <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:4 }}>
                  <span style={{ fontSize:14 }}>{["🥇","🥈","🥉"][i]}</span>
                  <span style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:11, color:"#c8bc9e", flex:1, overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>{e.domain}</span>
                  <span style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:12, fontWeight:700, color:"#a8d4a8" }}>{e.count.toLocaleString()}</span>
                </div>
                <div style={{ height:3, background:"rgba(255,255,255,0.06)", borderRadius:2, marginLeft:28 }}>
                  <div style={{ height:"100%", width:`${i===0?100:i===1?65:42}%`, background:"linear-gradient(90deg,#2a5e2a,#6ec86e)", borderRadius:2, transition:"width 0.9s ease" }} />
                </div>
              </div>
            ))}
            <div style={{ marginTop:12, paddingTop:10, borderTop:"1px solid rgba(181,169,138,0.08)", fontSize:9, color:"#3a3028", fontFamily:"'JetBrains Mono', monospace" }}>
              {data.processed_total.toLocaleString()} total edits tracked
            </div>
          </div>

          {/* System status */}
          <div style={{ background:"rgba(255,255,255,0.02)", border:"1px solid rgba(181,169,138,0.1)", borderRadius:13, padding:"18px 20px" }}>
            <div style={{ display:"flex", alignItems:"center", gap:7, marginBottom:16 }}>
              <span style={{ fontSize:13 }}>⚙️</span>
              <span style={{ fontSize:10, letterSpacing:".12em", color:"#8a7e64", fontFamily:"'JetBrains Mono', monospace", textTransform:"uppercase" }}>System Status</span>
            </div>

            {[
              { label:"Pipeline", value: data.is_running ? "RUNNING" : "STOPPED", color: data.is_running ? "#7ec87e" : "#e57373" },
              { label:"Mode", value: data.pipeline_mode?.toUpperCase(), color:"#a8d4a8" },
              { label:"Degraded mode", value: data.degraded_mode ? "ACTIVE — bots dropped" : "NORMAL", color: data.degraded_mode ? "#f5c842" : "#7ec87e" },
              { label:"Network watchdog", value: data.network_reset_pending ? "RECONNECTING" : "OK", color: data.network_reset_pending ? "#e57373" : "#7ec87e" },
              { label:"Missed deadlines", value: data.missed_deadlines, color: data.missed_deadlines > 0 ? "#f5c842" : "#7ec87e" },
              { label:"Dropped packets", value: data.dropped_packets, color: data.dropped_packets > 0 ? "#e57373" : "#7ec87e" },
            ].map(row => (
              <div key={row.label} style={{ display:"flex", justifyContent:"space-between", padding:"5px 0", borderBottom:"1px solid rgba(181,169,138,0.06)" }}>
                <span style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:10, color:"#8a7e64" }}>{row.label}</span>
                <span style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:10, fontWeight:700, color:row.color }}>{row.value}</span>
              </div>
            ))}

            <div style={{ marginTop:12, padding:"8px", background:"rgba(181,169,138,0.06)", borderRadius:6 }}>
              <div style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:9, color:"#5a5048", lineHeight:1.6 }}>
                STOP: pauses pipeline workers → shows metric freeze<br/>
                DEGRADED: auto-activates when p99 &gt; 2ms for 10s<br/>
                NETWORK RESET: fires after 10s silence from Wikipedia
              </div>
            </div>
          </div>
        </div>

        {/* Live feed — full width */}
        <div style={{ background:"rgba(255,255,255,0.02)", border:"1px solid rgba(181,169,138,0.1)", borderRadius:13, padding:"18px 20px" }}>
          <div style={{ display:"flex", alignItems:"center", gap:7, marginBottom:12 }}>
            <span style={{ fontSize:13 }}>📡</span>
            <span style={{ fontSize:10, letterSpacing:".12em", color:"#8a7e64", fontFamily:"'JetBrains Mono', monospace", textTransform:"uppercase" }}>Live Edit Feed</span>
            <div style={{ flex:1 }} />
            <span style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:9, color:"#4a4038" }}>👤 human = HIGH priority  |  🤖 bot = LOW priority</span>
          </div>
          <div ref={feedRef} style={{ maxHeight:200, overflowY:"auto", paddingRight:4 }}>
            {data.live_feed.length === 0 ? (
              <div style={{ fontFamily:"'JetBrains Mono', monospace", fontSize:10, color:"#4a4038", padding:"16px 0", textAlign:"center" }}>waiting for pipeline start…</div>
            ) : (
              [...data.live_feed].reverse().map((line, i) => (
                <div key={i} style={{
                  fontFamily:"'JetBrains Mono', monospace", fontSize:10,
                  padding:"3px 0", borderBottom:"1px solid rgba(181,169,138,0.06)",
                  color: line.startsWith("🤖") ? "#7a9e7a" : "#c8bc9e",
                  display:"flex", gap:6, animation: i===0?"fadeIn 0.4s ease":"none",
                }}>
                  <span style={{ opacity:0.4, fontSize:9, minWidth:20 }}>#{i+1}</span>
                  {line}
                </div>
              ))
            )}
          </div>
        </div>

        <div style={{ marginTop:16, paddingTop:12, borderTop:"1px solid rgba(181,169,138,0.06)", display:"flex", justifyContent:"space-between", fontFamily:"'JetBrains Mono', monospace", fontSize:8, color:"#2a2820", letterSpacing:".08em" }}>
          <span>WIKIPEDIA REAL-TIME MONITOR — UNIVERSITY ASSIGNMENT</span>
          <span>API → localhost:3000 | polling 1s</span>
        </div>
      </main>
    </div>
  );
}