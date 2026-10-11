"""Self-contained canvas map renderer for the grid underlay.

deck.gl/pydeck's TextLayer is broken against CJK (and even ASCII) in the
bundled deck.gl 9.3 stack (instanceIconDefs init error), so the map ships as a
plain HTML5-canvas component — same rendering approach as the offline preview
page (preview_mp.html), which handles Chinese labels, greedy label placement,
wheel-zoom and drag-pan entirely client-side (no per-rerun deck re-serialization).
"""
from __future__ import annotations

import json

# voltage class -> CSS color (mirrors the former pydeck palette)
_VOLT_CSS = [
    (1000000, "#780000"),
    (750000, "#c83c00"),
    (500000, "#d22828"),
    (330000, "#e69628"),
    (0, "#969696"),
]


def _css(v):
    for floor, c in _VOLT_CSS:
        if (v or 0) >= floor:
            return c
    return _VOLT_CSS[-1][1]


def build_grid_map_html(subs, lines, extent, ranked_names, max_pts: int = 60) -> str:
    """subs: DataFrame(name, max_voltage, lon, lat); lines: [{max_voltage, path}];
    extent: (lon_min, lon_max, lat_min, lat_max); ranked_names: PF node names to
    highlight in gold when a station name matches."""
    def _ds(path):
        if len(path) <= max_pts:
            return path
        step = len(path) / max_pts
        return [path[int(i * step)] for i in range(max_pts)]

    ranked = [r for r in ranked_names if len(r) >= 2]
    stations = []
    for r in subs.itertuples():
        name = r.name
        gold = any(q in name or name in q for q in ranked)
        mv = r.max_voltage
        mv = int(mv) if mv == mv and mv is not None else 0  # NaN-safe
        stations.append({"n": name, "x": round(r.lon, 5), "y": round(r.lat, 5),
                         "v": mv, "g": bool(gold)})
    line_data = [{"c": _css(p["max_voltage"]), "w": 2.2 if (p["max_voltage"] or 0) >= 750000 else 1.4,
                  "p": [[round(x, 4), round(y, 4)] for x, y in _ds(p["path"])]}
                 for p in lines]
    payload = {"ext": list(extent), "lines": line_data, "subs": stations}
    data_js = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    return _TEMPLATE.replace("__DATA__", data_js)


_TEMPLATE = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>
  html,body{margin:0;padding:0;background:#f2f1ea;font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif}
  #bar{display:flex;align-items:center;gap:14px;padding:4px 8px;font-size:12px;color:#444}
  #cv{display:block;width:100%;cursor:grab}
  .lg{display:inline-flex;align-items:center;gap:4px}
  .sw{width:16px;height:3px;display:inline-block}
</style></head><body>
<div id="bar">
  <label><input type="checkbox" id="lbl" checked> 站名标注</label>
  <span class="lg"><span class="sw" style="background:#780000"></span>1000kV</span>
  <span class="lg"><span class="sw" style="background:#c83c00"></span>750kV</span>
  <span class="lg"><span class="sw" style="background:#d22828"></span>500kV</span>
  <span class="lg"><span class="sw" style="background:#e69628"></span>330kV</span>
  <span style="color:#b8860b">●</span><span style="font-size:12px;color:#666">PF排名匹配</span>
  <span style="margin-left:auto;color:#999">滚轮缩放 · 拖拽平移 · 双击复位</span>
</div>
<canvas id="cv"></canvas>
<script>
const D = __DATA__;
const cv = document.getElementById('cv'), ctx = cv.getContext('2d');
const [x0,x1,y0,y1] = D.ext;
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
    ctx.strokeStyle = L.c; ctx.lineWidth = L.w; ctx.beginPath();
    let started=false;
    for(const [x,y] of L.p){
      const [a,b]=px(x,y);
      if(!started){ctx.moveTo(a,b);started=true;} else ctx.lineTo(a,b);
    }
    ctx.stroke();
  }
  for(const s of D.subs){
    const [a,b]=px(s.x,s.y);
    const r = s.g?7:(s.v>=750000?4.5:(s.v>=500000?3.2:2.2));
    ctx.beginPath(); ctx.arc(a,b,r,0,6.284);
    ctx.fillStyle = s.g?'#ffc300':(s.v>=750000?'#c83c00':(s.v>=500000?'#d22828':(s.v>=330000?'#e69628':'#969696')));
    ctx.fill(); ctx.lineWidth=1; ctx.strokeStyle='#2b2b2b'; ctx.stroke();
  }
  if(document.getElementById('lbl').checked){
    const zoomK = Math.max(0.45, Math.min(2.2, scale/60));
    const budget = Math.floor(60 * zoomK * 1.6);
    const placed = [];
    ctx.font = '11px sans-serif'; ctx.textAlign='center';
    const cand = D.subs.filter(s=>s.v>=500000).sort((a,b)=>b.v-a.v);
    let n=0;
    for(const s of cand){
      if(n>=budget) break;
      const [a,b]=px(s.x,s.y);
      if(a<0||a>W||b<0||b>H) continue;
      const w = ctx.measureText(s.n).width;
      const box = [a-w/2-2, b-16, a+w/2+2, b-4];
      let hit=false;
      for(const q of placed){ if(!(box[2]<q[0]||box[0]>q[2]||box[3]<q[1]||box[1]>q[3])){hit=true;break;} }
      if(hit) continue;
      placed.push(box); n++;
      ctx.fillStyle='rgba(242,241,234,0.75)';
      ctx.fillRect(box[0],box[1],box[2]-box[0],box[3]-box[1]);
      ctx.fillStyle = s.g?'#8a5a00':'#232323';
      ctx.fillText(s.n,a,b-5);
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
