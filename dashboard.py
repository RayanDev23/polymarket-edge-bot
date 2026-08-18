"""Local read-only PAPER dashboard.

The server exposes only GET endpoints and reads SQLite in read-only mode.
There is intentionally no order, signing, wallet, or configuration-control
route in this module.
"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from config import load_config
from monitoring import build_dashboard_payload, default_status_path


HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Polymarket Edge — PAPER Monitor</title>
<style>
:root{color-scheme:dark;--bg:#10141b;--panel:#19212c;--line:#2c3a4d;--muted:#9eafc3;--ok:#5ee19b;--warn:#f7c873;--bad:#ff7c85;--accent:#7dc6ff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:#edf4fb;font:14px/1.4 system-ui,-apple-system,Segoe UI,sans-serif}main{max-width:1500px;margin:auto;padding:20px}h1,h2{margin:0 0 12px}h1{font-size:24px}h2{font-size:17px;color:var(--accent)}section{margin:18px 0}.banner{border:2px solid var(--ok);color:var(--ok);font-weight:800;letter-spacing:2px;text-align:center;padding:10px;border-radius:8px;margin-bottom:16px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:10px}.panel{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:14px}.label{color:var(--muted);font-size:12px}.value{font-size:20px;font-weight:650;margin-top:2px}.small{font-size:12px;color:var(--muted)}.ok{color:var(--ok)}.warn{color:var(--warn)}.bad{color:var(--bad)}table{width:100%;border-collapse:collapse;font-size:12px}th,td{text-align:right;border-bottom:1px solid var(--line);padding:6px 5px;white-space:nowrap}th:first-child,td:first-child{text-align:left}canvas{width:100%;height:180px;background:#121923;border-radius:5px}.charts{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:10px}.mono{font-family:ui-monospace,SFMono-Regular,Consolas,monospace}.null{color:var(--muted)}@media(max-width:700px){main{padding:10px}table{display:block;overflow-x:auto}}
</style>
</head>
<body><main>
<div class="banner">PAPER ONLY — READ-ONLY MONITORING — NO REAL ORDERS</div>
<h1>Polymarket Edge / BTC 5m</h1>
<div class="small">Session: <span id="session" class="mono">—</span> · refreshes every 2 seconds</div>

<section><h2>Bot status</h2><div class="grid">
<div class="panel"><div class="label">Mode</div><div id="mode" class="value ok">PAPER ONLY</div></div>
<div class="panel"><div class="label">Binance</div><div id="binance" class="value">—</div><div id="binance-detail" class="small"></div></div>
<div class="panel"><div class="label">Polymarket WebSocket</div><div id="ws" class="value">—</div><div id="ws-detail" class="small"></div></div>
<div class="panel"><div class="label">CLOB / discovery</div><div id="clob" class="value">—</div><div id="clob-detail" class="small"></div></div>
<div class="panel"><div class="label">Uptime</div><div id="uptime" class="value">—</div><div id="last-message" class="small"></div></div>
<div class="panel"><div class="label">Active market</div><div id="market" class="value">—</div><div id="market-detail" class="small"></div></div>
</div></section>

<section><h2>BTC</h2><div class="grid">
<div class="panel"><div class="label">Binance BTC price</div><div id="btc-price" class="value">—</div><div id="btc-time" class="small"></div></div>
<div class="panel"><div class="label">Recent variation</div><div id="btc-var" class="value">—</div></div>
<div class="panel"><div class="label">Realized volatility</div><div id="vol" class="value">—</div><div id="vol-obs" class="small"></div></div>
</div></section>

<section><h2>Order book</h2><div class="grid">
<div class="panel"><h2>UP</h2><div id="up-book"></div></div>
<div class="panel"><h2>DOWN</h2><div id="down-book"></div></div>
<div class="panel"><h2>Current decision metrics</h2><div id="current-metrics"></div></div>
</div></section>

<section><h2>STRUCTURAL_ARB — analytical counters</h2><div id="structural-counters" class="grid"></div></section>
<section><h2>LATE_MARKET</h2><div class="grid"><div class="panel" id="late-current"></div><div class="panel" id="late-counters"></div></div></section>

<section><h2>Rejection analytics</h2><div class="panel"><table><thead><tr><th>Reason</th><th>Count</th><th>Percentage</th></tr></thead><tbody id="rejections"></tbody></table></div></section>
<section><h2>Live metrics — last 50 persisted opportunities</h2><div class="panel"><table><thead><tr><th>Timestamp</th><th>Remaining</th><th>Combined ask</th><th>Gross edge</th><th>Net edge</th><th>Fees</th><th>Slippage</th><th>Decision</th><th>Reason</th></tr></thead><tbody id="recent"></tbody></table></div></section>

<section><h2>Graphs</h2><div class="charts"><div class="panel"><div class="label">Combined ask</div><canvas id="chart-ask"></canvas></div><div class="panel"><div class="label">Gross edge</div><canvas id="chart-gross"></canvas></div><div class="panel"><div class="label">Net edge</div><canvas id="chart-net"></canvas></div><div class="panel"><div class="label">Time remaining</div><canvas id="chart-time"></canvas></div><div class="panel"><div class="label">BTC price</div><canvas id="chart-btc"></canvas></div></div></section>
</main>
<script>
const q=id=>document.getElementById(id), n=v=>v===null||v===undefined?'—':(typeof v==='number'?Number(v).toFixed(5):v), pct=v=>v===null||v===undefined?'—':(Number(v)*100).toFixed(3)+'%', date=v=>v?new Date(v).toLocaleTimeString():'—';
function statusClass(v){return String(v||'').toUpperCase()==='OK'?'ok':(String(v||'').toUpperCase()==='ERROR'?'bad':'warn')}
function show(id,value,cls){q(id).textContent=value??'—';if(cls)q(id).className='value '+cls}
function bookHtml(b){if(!b||!b.available)return '<span class="null">BOOK UNAVAILABLE</span>';return `<div>bid <b>${n(b.bid)}</b> · ask <b>${n(b.ask)}</b></div><div class="small">bid depth ${n(b.bid_depth)} · ask depth ${n(b.ask_depth)} · age ${n(b.age_ms)} ms · ${b.coherent?'coherent':'incoherent'}</div>`}
function counterCards(c){const labels=[['total_evaluations','Evaluations'],['ACCEPT','ACCEPT'],['REJECT','REJECT'],['combined_ask_lt_1.00','ask < 1.00'],['combined_ask_lt_0.995','ask < 0.995'],['combined_ask_lt_0.99','ask < 0.99'],['combined_ask_lt_0.98','ask < 0.98'],['gross_edge_gt_0','gross edge > 0'],['net_edge_gt_0','net edge > 0'],['net_edge_gt_0.001','net edge > .001'],['net_edge_gt_0.005','net edge > .005'],['net_edge_gt_0.01','net edge > .01']];return labels.map(([k,l])=>`<div class="panel"><div class="label">${l}</div><div class="value">${c&&c[k]??0}</div></div>`).join('')}
function render(s){const st=s.status||{}, b=st.binance||{}, ws=st.polymarket_websocket||{}, cl=st.clob||{}, m=st.market||{}, btc=st.btc||{}, books=st.order_book||{}, a=s.analytics||{}, sc=(a.structural_arb||{}).counters||{}, lc=(a.late_market||{}).counters||{}, cur=st.current_opportunity||{}, ca=(cur.analytics||{}).structural_arb||{}, la=(cur.analytics||{}).late_market||{};
q('session').textContent=s.session_id||'—';show('mode','PAPER ONLY','ok');show('binance',b.status||'WAITING',statusClass(b.status));q('binance-detail').textContent=`connected=${!!b.connected} · age=${n(b.age_ms)} ms · reconnects=${b.reconnects??0}`;show('ws',ws.status||'WAITING',statusClass(ws.status));q('ws-detail').textContent=`connected=${!!ws.connected} · reconnects=${ws.reconnects??0}`;show('clob',cl.status||'WAITING',statusClass(cl.status));q('clob-detail').textContent=`last success ${date(cl.last_success_at)}`;show('uptime',`${n(st.uptime_seconds)} s`);q('last-message').textContent=`last message ${date(st.last_message_at)}`;show('market',m.market_id||'NONE');q('market-detail').textContent=m?`${m.question||''} · ${date(m.start)} → ${date(m.end)} · ${n(m.remaining_seconds)} s`:'No active BTC 5m market';show('btc-price',n(btc.price));q('btc-time').textContent=date(btc.timestamp);show('btc-var',pct(btc.recent_variation));show('vol',n(btc.realized_volatility));q('vol-obs').textContent=`observations ${btc.volatility_observations??'—'}`;q('up-book').innerHTML=bookHtml(books.UP);q('down-book').innerHTML=bookHtml(books.DOWN);
q('current-metrics').innerHTML=`combined ask <b>${n(ca.combined_best_ask)}</b><br>gross edge <b>${n(ca.gross_edge_signed)}</b><br>net edge <b>${n(ca.net_edge??cur.net_edge)}</b><br>fees ${n(ca.fees_total??cur.estimated_fees)} · slippage ${n(ca.slippage_total??cur.estimated_slippage)}<br>capital ${n(ca.capital_required??cur.capital_required)} · target ${n(ca.target_quantity??cur.quantity)}<br><span class="small">${cur.decision||'—'} · ${cur.decision_reason||'—'}</span>`;
q('structural-counters').innerHTML=counterCards(sc);q('late-current').innerHTML=`<b>${la.price_to_beat===null||la.price_to_beat===undefined?'PRICE TO BEAT UNAVAILABLE':'PRICE TO BEAT AVAILABLE'}</b><br>price_to_beat ${n(la.price_to_beat)} · spot ${n(la.spot_price)}<br>probability ${n(la.model_probability)} · remaining ${n(la.remaining_seconds)}<br>volatility ${n(la.realized_volatility)} · observations ${la.volatility_observations??'—'}<br>candidate UP ${n(la.candidate_up&&la.candidate_up.net_edge)} · DOWN ${n(la.candidate_down&&la.candidate_down.net_edge)}<br>gross edge ${n(la.gross_edge)} · net edge ${n(la.net_edge)}<br><span class="small">${la.rejection_reason||la.decision||'—'}</span>`;q('late-counters').innerHTML=`evaluations <b>${lc.evaluations??0}</b><br>price_to_beat available ${lc.price_to_beat_available??0}<br>probabilities calculated ${lc.probability_calculated??0}<br>candidate signals ${lc.candidate_signal??0}<br>positive gross/net ${lc.positive_gross_edge??0} / ${lc.positive_net_edge??0}<br>ACCEPT ${lc.ACCEPT??0}`;
q('rejections').innerHTML=(s.rejections||[]).map(r=>`<tr><td>${r.reason}</td><td>${r.count}</td><td>${Number(r.percentage).toFixed(2)}%</td></tr>`).join('')||'<tr><td colspan="3">—</td></tr>';q('recent').innerHTML=(s.recent_observations||[]).slice().reverse().map(r=>`<tr><td>${date(r.timestamp)}</td><td>${n(r.remaining)}</td><td>${n(r.combined_ask)}</td><td>${n(r.gross_edge)}</td><td>${n(r.net_edge)}</td><td>${n(r.fees)}</td><td>${n(r.slippage)}</td><td>${r.decision||'—'}</td><td>${r.reason||'—'}</td></tr>`).join('')||'<tr><td colspan="9">No data for this session</td></tr>';
const rows=s.recent_observations||[];draw('chart-ask',rows.map(r=>r.combined_ask));draw('chart-gross',rows.map(r=>r.gross_edge));draw('chart-net',rows.map(r=>r.net_edge));draw('chart-time',rows.map(r=>r.remaining));draw('chart-btc',rows.map(()=>btc.price));}
function draw(id,values){const c=q(id),x=c.getContext('2d'),d=devicePixelRatio||1,w=c.clientWidth||300,h=180;c.width=w*d;c.height=h*d;x.setTransform(d,0,0,d,0,0);x.clearRect(0,0,w,h);const v=values.filter(z=>typeof z==='number'&&isFinite(z));if(!v.length){x.fillStyle='#9eafc3';x.fillText('No data',12,24);return}const min=Math.min(...v),max=Math.max(...v),span=max-min||1;x.strokeStyle='#7dc6ff';x.lineWidth=2;x.beginPath();v.forEach((z,i)=>{const px=8+i*(w-16)/Math.max(1,v.length-1),py=h-12-(z-min)*(h-24)/span;i?x.lineTo(px,py):x.moveTo(px,py)});x.stroke();x.fillStyle='#9eafc3';x.font='11px system-ui';x.fillText(max.toFixed(4),8,12);x.fillText(min.toFixed(4),8,h-2)}
async function refresh(){try{const r=await fetch('/api/state',{cache:'no-store'});if(r.ok)render(await r.json())}catch(e){show('clob','DASHBOARD OFFLINE','bad')}}refresh();setInterval(refresh,2000);window.addEventListener('resize',refresh);
</script></body></html>"""


class DashboardServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        address: tuple[str, int],
        database_path: str | Path,
        status_path: str | Path,
    ) -> None:
        self.database_path = Path(database_path)
        self.status_path = Path(status_path)
        super().__init__(address, DashboardHandler)


class DashboardHandler(BaseHTTPRequestHandler):
    server: DashboardServer

    def _send(self, content: bytes, content_type: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send(HTML.encode("utf-8"), "text/html; charset=utf-8")
            return
        if parsed.path == "/health":
            self._send(
                json.dumps({"ok": True, "mode": "PAPER", "paper_only": True}).encode(),
                "application/json; charset=utf-8",
            )
            return
        if parsed.path == "/api/state":
            requested = parse_qs(parsed.query).get("session_id", [None])[0]
            payload = build_dashboard_payload(
                self.server.database_path,
                self.server.status_path,
                requested,
            )
            self._send(
                json.dumps(payload, allow_nan=False).encode("utf-8"),
                "application/json; charset=utf-8",
            )
            return
        self._send(b"Not found", "text/plain; charset=utf-8", 404)

    def do_POST(self) -> None:  # noqa: N802 - explicitly refuse mutation routes
        self._send(b"Read-only dashboard", "text/plain; charset=utf-8", 405)

    def do_PUT(self) -> None:  # noqa: N802
        self._send(b"Read-only dashboard", "text/plain; charset=utf-8", 405)

    def do_DELETE(self) -> None:  # noqa: N802
        self._send(b"Read-only dashboard", "text/plain; charset=utf-8", 405)

    def log_message(self, *_args: object) -> None:
        return


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local read-only PAPER dashboard")
    config = load_config()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--database", default=str(config.database_path))
    parser.add_argument("--status-file")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    database_path = Path(args.database)
    status_path = Path(args.status_file) if args.status_file else default_status_path(database_path)
    server = DashboardServer((args.host, args.port), database_path, status_path)
    print(f"PAPER ONLY dashboard: http://{args.host}:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
