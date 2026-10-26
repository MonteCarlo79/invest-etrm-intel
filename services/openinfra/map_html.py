"""Self-contained canvas map renderer for the grid underlay.

deck.gl/pydeck's TextLayer is broken against CJK (and even ASCII) in the
bundled deck.gl 9.3 stack (instanceIconDefs init error), so the map ships as a
plain HTML5-canvas component — same rendering approach as the offline preview
page (preview_mp.html): match-semantics coloring (green = 网架图 station),
short Chinese labels only for matched + PF-ranked stations, greedy label
placement, wheel-zoom and drag-pan, all client-side.
"""
from __future__ import annotations

import json

from services.openinfra.match import match_vision, norm_name
from services.openinfra.vision_lists import VISION_LISTS

# sequential blue ramp for PF Rev/MW magnitude bands (light→dark)
_PF_BANDS = ["#dbe7f3", "#9ec4dd", "#5b9bc4", "#2673a8", "#0d4d80"]


def _line_style(name: str, max_voltage) -> tuple[str, float, bool]:
    """-> (css color, width, dashed). DC (换流/直流) purple dashed; else
    steel-blue shades by voltage class, matching the preview page."""
    if name and ("换流" in name or "直流" in name):
        return "#7a4fc9", 1.6, True
    v = max_voltage or 0
    if v >= 1000000:
        return "#16304f", 2.4, False
    if v >= 750000:
        return "#2c4a73", 2.0, False
    if v >= 500000:
        return "#5b7f96", 1.3, False
    return "#9db3c0", 1.0, False


def build_grid_map_html(subs, lines, extent, ranked_names, extent_tag: str,
                        pf_values: dict | None = None, max_pts: int = 60) -> tuple[str, dict]:
    """subs: DataFrame(name, max_voltage, lon, lat); lines: [{max_voltage, path}];
    ranked_names: PF node names to ring in gold when a station matches;
    pf_values: {pf_node_name: rev_per_mw} — stations whose name core matches a
    PF node get a sequential blue band by value (darker = higher Rev/MW).
    Returns (html, counts) where counts = {ok, collapsed, missing, pf_matched}."""
    def _ds(path):
        if len(path) <= max_pts:
            return path
        step = len(path) / max_pts
        return [path[int(i * step)] for i in range(max_pts)]

    matched = match_vision(VISION_LISTS.get(extent_tag, []), subs)
    vision_total = len(VISION_LISTS.get(extent_tag, []))

    # PF node -> station: strip 内蒙./蒙东. prefix + /220kV.xM suffix, normalize,
    # then match onto station normalized names (exact or containment)
    pf_by_station: dict[str, float] = {}
    if pf_values:
        by_norm = {}
        for r in subs.itertuples():
            k = norm_name(r.name)
            if k:
                by_norm[k] = r.name
        for node, val in pf_values.items():
            # 内蒙.德岭山站/220kV.1M → strip /voltage part FIRST (it contains a
            # dot), then the province prefix, then normalize
            core = norm_name(str(node).split("/")[0].split(".")[-1])
            if not core:
                continue
            if core in by_norm:
                pf_by_station[by_norm[core]] = float(val)
                continue
            cands = [k for k in by_norm if len(k) >= 3 and (k in core or core in k)]
            if cands:
                pf_by_station[by_norm[min(cands, key=len)]] = float(val)
    pf_vals = list(pf_by_station.values())
    if pf_vals:
        pf_vals.sort()
        qs = [pf_vals[int(len(pf_vals) * q)] for q in (0.2, 0.4, 0.6, 0.8)]
    else:
        qs = []

    def _band(v: float) -> int:
        for i, q in enumerate(qs):
            if v <= q:
                return i
        return len(_PF_BANDS) - 1

    counts = {
        "ok": sum(1 for m in matched.values() if m["status"] == "OK"),
        "collapsed": sum(1 for m in matched.values() if m["status"] == "COLLAPSED"),
        "missing": vision_total - len(matched),
        "pf_matched": len(pf_by_station),
    }

    ranked = [r for r in ranked_names if len(r) >= 2]
    stations = []
    for r in subs.itertuples():
        name = r.name
        gold = any(q in name or name in q for q in ranked)
        m = matched.get(name)
        mv = r.max_voltage
        mv = int(mv) if mv == mv and mv is not None else 0  # NaN-safe
        pfv = pf_by_station.get(name)
        if gold:
            kind = "gold"
        elif pfv is not None:
            kind = "pf"
        elif m and m["status"] == "OK":
            kind = "ok"
        elif m:
            kind = "collapsed"
        else:
            kind = "other"
        stations.append({"n": name, "x": round(r.lon, 5), "y": round(r.lat, 5),
                         "v": mv, "k": kind, "b": _band(pfv) if pfv is not None else -1,
                         "pf": round(pfv) if pfv is not None else None,
                         "lbl": (m["vision"] if m else name)})
    line_data = []
    for p in lines:
        css, w, dash = _line_style(p.get("name", ""), p["max_voltage"])
        line_data.append({"c": css, "w": w, "d": dash,
                          "p": [[round(x, 4), round(y, 4)] for x, y in _ds(p["path"])]})
    payload = {"ext": list(extent), "lines": line_data, "subs": stations}
    data_js = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return _TEMPLATE.replace("__DATA__", data_js), counts


_TEMPLATE = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>
  html,body{margin:0;padding:0;background:#f2f1ea;font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif}
  #bar{display:flex;align-items:center;gap:12px;padding:4px 8px;font-size:12px;color:#444;flex-wrap:wrap}
  #cv{display:block;width:100%;cursor:grab}
  .lg{display:inline-flex;align-items:center;gap:4px}
  .sw{width:16px;height:3px;display:inline-block}
</style></head><body>
<div id="bar">
  <label><input type="checkbox" id="lbl" checked> 站名标注</label>
  <span class="lg"><span class="sw" style="background:#5b7f96"></span>500kV线路</span>
  <span class="lg"><span class="sw" style="background:#2c4a73"></span>750kV+线路</span>
  <span class="lg"><span class="sw" style="background:#7a4fc9"></span>直流</span>
  <span style="color:#2e8b57">●</span><span style="font-size:12px;color:#666">网架图站(匹配)</span>
  <span style="color:#b8a02e">●</span><span style="font-size:12px;color:#666">归并</span>
  <span style="color:#8a8f98">●</span><span style="font-size:12px;color:#666">其他命名站</span>
  <span style="color:#ffc300">●</span><span style="font-size:12px;color:#666">PF排名匹配</span>
  <span class="lg"><span class="sw" style="background:#dbe7f3"></span><span class="sw" style="background:#5b9bc4"></span><span class="sw" style="background:#0d4d80"></span> PF Rev/MWh (深=高)</span>
  <span style="margin-left:auto;color:#999">滚轮缩放 · 拖拽平移 · 双击复位</span>
</div>
<canvas id="cv"></canvas>
<script>
const D = __DATA__;
const cv = document.getElementById('cv'), ctx = cv.getContext('2d');
const [x0,x1,y0,y1] = D.ext;
const KIND_COLOR = {ok:'#2e8b57', collapsed:'#b8a02e', other:'#8a8f98', gold:'#ffc300'};
const PF_BANDS = ['#dbe7f3','#9ec4dd','#5b9bc4','#2673a8','#0d4d80'];
let W,H,scale,ox,oy;
function fit(){
  const r = cv.parentElement.getBoundingClientRect();
  W = cv.width = r.width; H = cv.height = 560;
  const pad = 30;
  scale = Math.min((W-2*pad)/(x1-x0),(H-2*pad)/(y1-y0));
  ox = pad + ((W-2*pad)-scale*(x1-x0))/2 - scale*x0;
  oy = pad + ((H-2*pad)-scale*(y1-y0))/2 + scale*y1;
}
function px(x,y){ return [ox+scale*x, oy-scale*y]; }
function draw(){
  ctx.fillStyle = '#f2f1ea'; ctx.fillRect(0,0,W,H);
  ctx.lineCap='round';
  for(const L of D.lines){
    ctx.strokeStyle = L.c; ctx.lineWidth = L.w;
    ctx.setLineDash(L.d?[6,4]:[]); ctx.beginPath();
    let started=false;
    for(const [x,y] of L.p){
      const [a,b]=px(x,y);
      if(!started){ctx.moveTo(a,b);started=true;} else ctx.lineTo(a,b);
    }
    ctx.stroke();
  }
  ctx.setLineDash([]);
  for(const s of D.subs){
    const [a,b]=px(s.x,s.y);
    if(a<-10||a>W+10||b<-10||b>H+10) continue;
    const isPf = s.k==='pf';
    const r = s.k==='gold'?6.5:(isPf?5.5:(s.k==='ok'?4:(s.k==='collapsed'?3.4:2.4)));
    ctx.beginPath(); ctx.arc(a,b,r,0,6.284);
    ctx.fillStyle = isPf ? PF_BANDS[s.b] : KIND_COLOR[s.k];
    ctx.fill(); ctx.lineWidth=1; ctx.strokeStyle='#2b2b2b'; ctx.stroke();
    if(s.k==='gold'){ ctx.beginPath(); ctx.arc(a,b,r+2.5,0,6.284);
      ctx.lineWidth=1.6; ctx.strokeStyle='#b8860b'; ctx.stroke(); }
  }
  if(document.getElementById('lbl').checked){
    const zoomK = Math.max(0.45, Math.min(2.2, scale/60));
    const budget = Math.floor(80 * zoomK * 1.6);
    const placed = [];
    ctx.font = '12px sans-serif'; ctx.textAlign='center';
    const cand = D.subs.filter(s=>s.k!=='other')
                       .sort((a,b)=>(a.k==='gold'?-1:0)-(b.k==='gold'?-1:0)
                              || (a.k==='pf'?-1:0)-(b.k==='pf'?-1:0) || b.v-a.v);
    let n=0;
    for(const s of cand){
      if(n>=budget) break;
      const [a,b]=px(s.x,s.y);
      if(a<0||a>W||b<0||b>H) continue;
      const text = s.pf!=null ? s.lbl+' · '+s.pf : s.lbl;
      const w = ctx.measureText(text).width;
      const box = [a-w/2-2, b-17, a+w/2+2, b-4];
      let hit=false;
      for(const q of placed){ if(!(box[2]<q[0]||box[0]>q[2]||box[3]<q[1]||box[1]>q[3])){hit=true;break;} }
      if(hit) continue;
      placed.push(box); n++;
      ctx.fillStyle='rgba(242,241,234,0.78)';
      ctx.fillRect(box[0],box[1],box[2]-box[0],box[3]-box[1]);
      ctx.fillStyle = s.k==='gold'?'#8a5a00':(s.k==='pf'?'#0d4d80':(s.k==='ok'?'#1d5c38':(s.k==='collapsed'?'#7a6520':'#555')));
      ctx.fillText(text,a,b-5);
    }
  }
}
let drag=null;
cv.addEventListener('mousedown',e=>{drag={x:e.offsetX,y:e.offsetY};cv.style.cursor='grabbing';});
window.addEventListener('mouseup',()=>{drag=null;cv.style.cursor='grab';});
window.addEventListener('mousemove',e=>{ if(!drag) return;
  const r=cv.getBoundingClientRect();
  ox+= (e.clientX-r.left)-drag.x; oy += (e.clientY-r.top)-drag.y;
  drag={x:e.clientX-r.left,y:e.clientY-r.top}; draw();});
cv.addEventListener('wheel',e=>{ e.preventDefault();
  const k = e.deltaY<0?1.15:1/1.15, mx=e.offsetX,my=e.offsetY;
  const ns = Math.max(5, Math.min(4000, scale*k)); const q=ns/scale;
  ox = mx - (mx-ox)*q; oy = my - (my-oy)*q; scale=ns; draw();},{passive:false});
cv.addEventListener('dblclick',()=>{fit();draw();});
window.addEventListener('resize',()=>{fit();draw();});
document.getElementById('lbl').addEventListener('change',draw);
fit(); draw();
</script></body></html>"""
