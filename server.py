#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cumora 实时指挥中心 · 本地数据服务
- 解析 ~/.cumora/daemon.log（增量），提取：任务(turn 生命周期) / 产出物 / 消息全文 / 群归属 / token 用量
- 提供 GET /api/snapshot（最近 4 小时窗口 + token 全量累计），CORS 开放
- 托管同目录 Cumora指挥中心.html
用法: python3 server.py [port]   (默认 8787)
"""
import re, json, os, time, threading, sys, hashlib, subprocess, calendar, queue, mimetypes
from urllib.parse import urlparse, parse_qs
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, timezone

# ===== 极简部署：全部可环境变量覆盖（一个 .env / 一条命令起服务，BRAM #D）=====
BASE = os.path.dirname(os.path.abspath(__file__))
LOG  = os.path.expanduser(os.environ.get("CUMORA_LOG", "~/.cumora/daemon.log"))
LOGS = [LOG + ".1", LOG]   # 轮转后的旧主文件在前，当前文件在后
# 页面文件名：优先 $CC_HTML；其次 index.html（发布包用）；再次中文名（本机原文件）
_html_env = os.environ.get("CC_HTML")
HTML_CANDIDATES = ([_html_env] if _html_env else []) + ["index.html", "Cumora指挥中心.html"]
PORT = int(os.environ.get("CC_PORT", sys.argv[1] if len(sys.argv) > 1 else 8787))
WINDOW_H = 4 * 3600          # 最近 4 小时
MAX_EV   = 1200              # 事件上限
MAX_MSG  = 400               # 消息流上限
MAX_TASK = 120               # 任务面板上限

GID2SHORT = {"g-ec7538ec": "eda", "g-c4463f63": "emag", "g-5b78687a": "galaxy"}
SHORT2NAME = {"eda": "嘉立创EDA PCB设计群", "emag": "电磁仿真项目组",
              "galaxy": "银河战舰游戏项目", "priv": "私聊 / 单对单", "oth": "其他",
              "g-e24ceb6d": "星际指挥中心"}
AGENT_SHORT = ("nina", "nova", "iris", "atlas", "sage", "bram", "peta")

def ash(agent):
    a = agent
    for suf in ("-4idw", "-94d8", "-9366", "-ff58", "-6hig", "-500f", "-j4og", "-8kqd"):
        a = a.replace(suf, "")
    return a if a in AGENT_SHORT else a[:12]

def gshort(gid):
    if not gid:
        return ""
    if gid.startswith("direct-") or gid == "priv":
        return "priv"
    return GID2SHORT.get(gid, gid)   # 未知群用原始 gid，前端动态生成星球

PAT_H = re.compile(r'^\s*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)\s*\[computer\]\s*([a-z0-9-]+)\s+(.*)$')
PAT_O = re.compile(r'^\s*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)\s+\[([a-z0-9-]+)/opencode\]\s+(\{.*\})$')
TOK_RE = re.compile(r'"tokens":\{"total":\d+,"input":(\d+),"output":(\d+),"reasoning":(\d+)')
PATH_RE = re.compile(r'(/Users/[A-Za-z0-9_\-./]+(?:\.[A-Za-z0-9]{1,5})?)')
REL_PATH_RE = re.compile(r'([A-Za-z0-9_\-./]{3,}\.(?:pdf|md|py|html|json|c|cpp|h|js|jsx|ts|xlsx|docx|png|jpg|jpeg|svg|zip|txt|ino|yaml|yml))')

FP_RE = re.compile(r'"callID":"([^"]+)"|"part":\{"id":"([^"]+)"|"messageID":"([^"]+)"')
def fingerprint(line):
    """行级幂等指纹：唯一 id（callID/part.id/messageID）优先，否则内容哈希兜底。
    不用 sessionID 当主键（同会话多行相同会误判重复跳过）。"""
    m = FP_RE.search(line)
    if m:
        for g in m.groups():
            if g: return g
    return hashlib.sha1(line.encode("utf-8", "ignore")).hexdigest()[:24]

class State:
    """流式日志解析状态（增量安全）"""
    def __init__(self):
        self.ptr = {}          # agent -> group 短名（最近已知群）
        self.turn = {}         # agent -> task dict（进行中）
        self.tasks = {}        # task_id -> task
        self.last_task = {}    # agent -> 最近一次任务 id（乱序日志兜底关联）
        self.comm = []         # 窗口内 agent 间沟通（{from,to,t,ts}，升序）
        self.boss_mentions = []  # 窗口内提到用户（BOSS）的事件（{from,t,ts}，升序）
        self.messages = []     # 窗口内消息（新->旧）
        self.artifacts = []    # 窗口内产出物
        self.tok_g = {}        # group -> agent -> {i,o,r,steps}
        self.tok_all = {}      # agent -> {i,o,r,steps}
        self.tok_total = {"i":0,"o":0,"r":0,"steps":0}
        self.events = []       # 窗口内事件（新->旧）
        self.seq = 0
        self.groups = {}      # 群短名 -> {name, cnt}（动态群发现，按活动次数）
        self.now = time.time()

    def _track_group(self, g, raw=""):
        """记录群活动（动态群发现）；g 为短名（已知映射或原始 gid）"""
        d = self.groups.setdefault(g, {"name": SHORT2NAME.get(g, g), "cnt": 0})
        d["cnt"] += 1

    def _add_tok(self, agent, g, i, o, r):
        ta = self.tok_all.setdefault(agent, {"i":0,"o":0,"r":0,"steps":0})
        for k,v in (("i",i),("o",o),("r",r)):
            ta[k]+=v
        ta["steps"]+=1
        self.tok_total["i"]+=i; self.tok_total["o"]+=o; self.tok_total["r"]+=r
        self.tok_total["steps"]+=1
        if g:
            d = self.tok_g.setdefault(g, {})
            tg = d.setdefault(agent, {"i":0,"o":0,"r":0,"steps":0})
            tg["i"]+=i; tg["o"]+=o; tg["r"]+=r; tg["steps"]+=1

    def _trim(self):
        cutoff = self.now - WINDOW_H
        self.events = [e for e in self.events if e["ts"] >= cutoff][-MAX_EV:]
        self.messages = [m for m in self.messages if m["ts"] >= cutoff][-MAX_MSG:]
        self.artifacts = [a for a in self.artifacts if a["ts"] >= cutoff][-200:]
        self.comm = [c for c in self.comm if c["ts"] >= cutoff][-200:]
        self.boss_mentions = [b for b in self.boss_mentions if b["ts"] >= cutoff][-50:]
        kt = {}
        for tid, tk in self.tasks.items():
            if tk["status"] != "running" and tk["end_ts"] and tk["end_ts"] < cutoff:
                continue
            kt[tid] = tk
        self.tasks = dict(list(kt.items())[-MAX_TASK:])

    def feed(self, line):
        hm = PAT_H.match(line)
        if hm:
            iso, agent, rest = hm.group(1), hm.group(2), hm.group(3)
            hms = iso[11:19]
            g = ""
            gid = re.search(r'g-[a-f0-9]{8}', rest)
            cv = re.search(r'convo=(\S+)', rest)
            if gid:
                g0 = gid.group(0)
                self._track_group(gshort(g0), rest)
                self.ptr[agent] = gshort(g0)
            elif cv:
                c = cv.group(1)
                g = gshort(c) if c.startswith("g-") else ("priv" if c.startswith("direct-") else "priv")
                self._track_group(g, rest)
                self.ptr[agent] = g
            g = self.ptr.get(agent, "")
            ts = iso_ts(iso)
            # BRAM:groups-last-ts —— 每群最近一次 turn 活动的 epoch（add-only，供前端判「7 天活跃」）
            if g and g in self.groups and ts > self.groups[g].get("last_ts", 0):
                self.groups[g]["last_ts"] = ts
            if "turn START" in rest:
                seq = self.seq + 1; self.seq = seq
                reason = "message.new"
                rm = re.search(r'sse-wake:(\S+)', rest)
                if rm: reason = rm.group(1).rstrip(")")
                self.turn[agent] = {
                    "id": "t-%d" % seq, "agent": ash(agent), "group": g,
                    "reason": reason, "start": hms, "start_ts": ts,
                    "end": None, "end_ts": None, "dur_s": None,
                    "status": "running", "title": "", "steps": 0,
                    "tools": [], "artifacts": [], "msgs": 0, "reset": False,
                }
                self.last_task[agent] = "t-%d" % seq
                self.events.append({"t": hms, "ts": ts, "a": ash(agent), "e": "task_open",
                                    "m": "%s 唤醒(%s)" % (ash(agent), reason), "g": g})
            elif "turn DONE" in rest:
                tk = self.turn.pop(agent, None)
                if tk:
                    tk["end"] = hms; tk["end_ts"] = ts
                    tk["dur_s"] = max(1, int((ts - tk["start_ts"]) * 10) / 10)
                    tk["status"] = "done"
                    if not tk["title"]: tk["title"] = "（无摘要）"
                    self.tasks[tk["id"]] = tk
                    self.events.append({"t": hms, "ts": ts, "a": ash(agent), "e": "task_close",
                                        "m": "%s 关闭 %s · %.0fs" % (ash(agent), tk["id"], tk["dur_s"]), "g": g})
            elif "FRESH engine session" in rest or "cleared opencode session" in rest:
                tk = self.turn.get(agent)
                if tk: tk["reset"] = True
            return
        om = PAT_O.match(line)
        if not om:
            return
        iso, agent, body = om.group(1), om.group(2), om.group(3)
        hms = iso[11:19]
        g = self.ptr.get(agent, "")
        agent_s = ash(agent)
        ts = iso_ts(iso)
        tk = self.turn.get(agent)
        if tk is None:
            lt = self.last_task.get(agent)
            if lt and lt in self.tasks:
                tk = self.tasks[lt]
        if '"type":"text"' in body:
            txt = ""
            try:
                o = json.loads(body)
                txt = (o.get("part") or {}).get("text") or o.get("text") or ""
            except Exception:
                m = re.search(r'"text":\s*"((?:[^"\\]|\\.){0,600})', body)
                if m:
                    txt = m.group(1).replace("\\n", " ").replace("\\r", " ").replace("\\t", " ")
            if txt:
                if tk:
                    tk["msgs"] += 1
                    if not tk["title"]:
                        tk["title"] = txt[:46]
                # P0 换源：messages / comm / boss_mentions 改由 load_real()（真实 Cumora 消息）提供，
                # 此处不再落 self.messages/self.comm/self.boss_mentions（避免旁白混入）。
                self.events.append({"t": hms, "ts": ts, "a": agent_s, "e": "text",
                                    "m": txt[:90], "g": g, "full": txt})
                # 产出物：文本里的文件路径
                for p in PATH_RE.findall(txt) + REL_PATH_RE.findall(txt):
                    p = p.rstrip(".,;:)")
                    if len(p) > 6:
                        self.artifacts.append({"t": hms, "ts": ts, "a": agent_s, "g": g, "path": p})
                        if tk: tk["artifacts"].append(p)
        elif '"type":"tool_use"' in body:
            tm = re.search(r'"type":"tool","tool":"([a-zA-Z0-9_\-]+)"', body)
            tool = tm.group(1) if tm else "?"
            if tk:
                tk["tools"].append(tool)
                tk["steps"] += 1
            im = re.search(r'"input":\{(.{0,260})', body)
            inp = im.group(1) if im else ""
            if tool in ("write", "edit"):
                fm = re.search(r'"filePath":"((?:[^"\\]|\\.){1,260})"', inp)
                if fm:
                    path = fm.group(1).replace("\\/", "/")
                    self.artifacts.append({"t": hms, "ts": ts, "a": agent_s, "g": g, "path": path})
                    if tk: tk["artifacts"].append(path)
        elif '"type":"step_finish"' in body:
            tm = TOK_RE.search(body)
            if tm:
                i, o, r = int(tm.group(1)), int(tm.group(2)), int(tm.group(3))
                self._add_tok(agent_s, g, i, o, r)
                if tk: tk["steps"] += 1
        elif "RATE-LIMITED" in body:
            self.events.append({"t": hms, "ts": ts, "a": agent_s, "e": "rate_limit", "m": "限流冷却", "g": g})
        elif "APIError" in body or "ContextOverflow" in body or "No endpoints" in body:
            self.events.append({"t": hms, "ts": ts, "a": agent_s, "e": "error", "m": "上游错误", "g": g})

def iso_ts(iso):
    try:
        return datetime.strptime(iso[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return time.time()

# ===== P0 换源：真实 Cumora 消息（`cumora messages`），替代 opencode 内部旁白 type:text =====
# 为什么：daemon.log 的 "type":"text" 是 agent 内部自述稿，不是真实群消息；真消息走 cumora reply，
# 正文多经 --file（正文不在日志），日志只留 sent(m-…, seq N)。故消息/提及一律换源到 CLI 结构化真身。
CONVO_FALLBACK = ("g-e24ceb6d", "g-c4463f63", "g-ec7538ec", "g-5b78687a")
REAL_TTL = 20.0                 # 真消息缓存秒数（略大于前端 15s 轮询，减少冷刷子进程抖动）
# ★ 2026-10-22 Bram（card-e9e8bdf4-c09 · @sage-6hig 04:5xZ 订正归因 + @nova-94d8 05:0xZ 裁）：
#   **可见窗口被"条数上限"挤短，不是 tailer 丢**（消息列表走 `cumora messages --tail N`，
#   tailer 只喂日志侧的事件/产出物/token）⇒ 旧值 N=60 ⇒ 繁忙会话 4h 内的 122 条只剩 60 条
#   ⇒ 最早消息只剩 ~1h55m 前 ⇒ **广告写着 4h、实际给 1h55m**（= 静默骗人）。
#   ⇒ 现在 N=500（覆盖 4h 窗口；`--tail` 取不到更多时由下面的 4h 过滤兜底），
#     并把**实际覆盖时长**如实报出去（snapshot 的 msg_window_h / msg_tail_hit）⇒
#     窗口被上限挤短时前端能显示真实值，而不是继续宣称 4h。
REAL_TAIL = int(os.environ.get("CC_MSG_TAIL", "500"))   # 每会话取最近 N 条
_real_cache = {"t": 0.0, "data": None}
MSG_LINE_RE = re.compile(r'^\s*\[(m-[0-9a-f-]+)\]\s*\[\s*(\d{1,2}):(\d{2})\s*(AM|PM)\]\s+([A-Za-z0-9_-]+)\s*#\s*(\d+)\s+(.*)$')

def _active_convos():
    """最近 4h 内有 turn 活动的会话（扫 daemon.log 尾部识别）；失败/空则回退已知群。"""
    cutoff = time.time() - WINDOW_H
    found = OrderedDict()
    try:
        size = os.path.getsize(LOG)
        with open(LOG, "rb") as f:
            f.seek(max(0, size - 3_000_000))
            data = f.read().decode("utf-8", "ignore")
        for ln in data.splitlines():
            m = re.match(r'^\s*(\d{4}-\d{2}-\d{2}T[0-9:.]+Z).*?\bfor ((?:g|allhands)-[a-z0-9-]+)', ln)
            if m and iso_ts(m.group(1)) >= cutoff:
                found[m.group(2)] = None
    except Exception:
        found = OrderedDict()
    return list(found.keys()) or list(CONVO_FALLBACK)

def _fetch_convo(cid):
    """拉单个会话的最近消息文本（CLI 结构化真身）；任何失败都返回空串，不抛。

    返回 (cid, 文本, 拉到的行数) ⇒ 行数用于判**条数上限有没有顶到**
    （顶到 = 这个会话的可见窗口被上限挤短 ⇒ 要如实报出来，不能继续宣称 4h）。
    """
    try:
        r = subprocess.run(["cumora", "messages", cid, "--tail", str(REAL_TAIL)],
                           capture_output=True, text=True, timeout=10)
        _txt = r.stdout or ""
        return cid, _txt, len(_txt.splitlines())
    except Exception:
        return cid, "", 0

def load_real():
    """真实群消息 → messages / comm / boss_mentions（TTL 缓存；失败回退上次好数据）。
       时间戳只有时钟无日期：按同群消息时序从新到旧单调回推（时钟变大=跨天回退一天）重建。"""
    now = time.time()
    if _real_cache["data"] is not None and now - _real_cache["t"] < REAL_TTL:
        return _real_cache["data"]
    gm = time.gmtime(now)
    midnight = calendar.timegm((gm.tm_year, gm.tm_mon, gm.tm_mday, 0, 0, 0, 0, 0, 0))
    now_clock = now - midnight
    convos = _active_convos()
    try:
        with ThreadPoolExecutor(max_workers=max(1, len(convos))) as ex:
            outs = list(ex.map(_fetch_convo, convos))
    except Exception:
        outs = [_fetch_convo(c) for c in convos]
    # 条数上限有没有顶到（顶到 ⇒ 该会话的可见窗口被上限挤短 ⇒ 如实报，不装作 4h）
    _tail_hit = any(n >= REAL_TAIL for (_c, _t, n) in outs)

    msgs, comm, boss = [], [], []
    for cid, out, _nlines in outs:
        g = gshort(cid)
        rows = []
        for line in (out or "").splitlines():
            m = MSG_LINE_RE.match(line)
            if not m:
                continue
            _mid, hh, mm, ap, sender, _seq, body = m.groups()
            clock = (int(hh) % 12 + (12 if ap == "PM" else 0)) * 3600 + int(mm) * 60
            rows.append((clock, sender, body))
        ts_list = [0] * len(rows)
        prev_clock = None; prev_day = 0
        for idx in range(len(rows) - 1, -1, -1):
            clock = rows[idx][0]
            if prev_clock is None:
                day = 0 if clock <= now_clock + 120 else -1
            else:
                day = prev_day - (1 if clock > prev_clock else 0)
            ts_list[idx] = midnight + day * 86400 + clock
            prev_clock = clock; prev_day = day
        for idx, (_clock, sender, body) in enumerate(rows):
            ts = ts_list[idx]
            if ts < now - WINDOW_H:                 # 4h 窗口外丢弃
                continue
            a = "you" if sender.startswith("u-") else ash(sender)
            hm = time.strftime("%H:%M:%S", time.gmtime(ts))
            txt = body.replace("\\n", " ").replace("\\r", " ").replace("\\t", " ").strip()
            if not txt:
                continue
            _sys = ('"kind":"notice"' in txt) or ("byoa_engine_failed" in txt)
            msgs.append({"t": hm, "a": a, "g": g, "txt": txt, "ts": ts, "sys": _sys})
            seen = set()
            for m2 in re.finditer(r'@([A-Za-z0-9][A-Za-z0-9-]{1,19})', txt):
                raw = m2.group(1).lower()
                if raw == "you" or raw.startswith("u-"):
                    if "you" not in seen and a != "you":
                        seen.add("you"); comm.append({"from": a, "to": "you", "t": hm, "ts": ts})
                    continue
                tgt = ash(raw)
                if tgt and tgt != a and tgt in AGENT_SHORT and tgt not in seen:
                    seen.add(tgt); comm.append({"from": a, "to": tgt, "t": hm, "ts": ts})
            for name in AGENT_SHORT:
                if name == a or name in seen:
                    continue
                if re.search(r'(?<![a-z0-9])' + name + r'(\s#\d+|\x27s|\u2019s|\s)', txt, re.I):
                    comm.append({"from": a, "to": name, "t": hm, "ts": ts})
            if a != "you" and re.search(r'@you\b|@u-[a-f0-9]+|\bbatyvn\b', txt):
                boss.append({"from": a, "t": hm, "ts": ts})
    msgs.sort(key=lambda x: x["ts"])
    # 实际覆盖时长（窗口内最早一条到现在）⇒ 前端据此显示真实窗口，别只宣称 window_h
    _span_h = 0.0
    if msgs:
        _span_h = max(0.0, (now - min(m["ts"] for m in msgs)) / 3600.0)
    data = {"messages": msgs[-MAX_MSG:], "comm": comm, "boss_mentions": boss,
            "msg_window_h": round(_span_h, 3), "msg_tail_cap": REAL_TAIL,
            "msg_tail_hit": bool(_tail_hit)}
    _real_cache["t"] = now
    _real_cache["data"] = data
    return data

def parse_files(st, paths, seen=None):
    """按序读取多个日志文件（.1 旧 → 当前新），逐行喂状态机；seen 为指纹集合时同步填充"""
    for p in paths:
        try:
            with open(p, "rb") as f:
                chunk = f.read()
        except FileNotFoundError:
            continue
        for ln in chunk.decode("utf-8", errors="ignore").splitlines():
            if seen is not None:
                seen[fingerprint(ln)] = None
                if len(seen) > 40000:
                    seen.popitem(last=False)
            st.feed(ln)

st = State()
seen = OrderedDict()      # 行指纹（LRU 上限 40000），ingest 幂等去重
lock = threading.Lock()
_log_ino = None

def full_scan():
    """全量重建：启动 / 文件轮转时调用；重建期间同时填充指纹集合"""
    global st, seen, _log_ino
    with lock:
        st = State()
        seen = OrderedDict()
        st.now = time.time()
        parse_files(st, LOGS, seen=seen)
        st._trim()
        try:
            _log_ino = os.stat(LOG).st_ino
        except OSError:
            _log_ino = None

def check_rotate():
    """快照前检测 daemon.log 轮转（inode 变化）→ 重建；pusher 异常时兜底"""
    global _log_ino
    try:
        ino = os.stat(LOG).st_ino
    except OSError:
        return False
    if _log_ino is not None and ino != _log_ino:
        full_scan()
        return True
    _log_ino = ino
    return False

def ingest_lines(lines):
    """pusher 推来的新日志行：指纹去重后增量喂入状态机"""
    got = skip = 0
    with lock:
        for ln in lines:
            fp = fingerprint(ln)
            if fp in seen:
                skip += 1
                continue
            seen[fp] = None
            if len(seen) > 40000:
                seen.popitem(last=False)
            st.feed(ln)
            got += 1
        st.now = time.time()
        st._trim()
    return {"accepted": got, "skipped": skip}

# ===== Phase 0/1（BRAM 2026-10-16，Nova 批）：进程内 durable tail + SSE 推送 =====
# 目标：① 不再依赖外部 pusher / 手动重启（进程内跟 daemon.log 追加，只读新增字节）
#       ② 变更即推（SSE /api/stream），前端可省轮询
#       ③ 失败可见（Sage D 硬门）：tail 反复失败 → mode="poll" + health 字段暴露，
#          宁可可见地退回周期性全量，也**不静默冻结**。
SSE_SUBS = set()
SSE_LOCK = threading.Lock()
REV = {"n": 0}
HEALTH = {"tail_alive": False, "tail_mode": "starting", "tail_last_read": 0.0,
          "tail_last_change": 0.0, "tail_lines": 0, "tail_err": None, "_last_poll": 0.0}

def bump():
    """任一数据变更：rev+1 并唤醒所有 SSE 订阅者，同时记 last_change"""
    with SSE_LOCK:
        REV["n"] += 1
        HEALTH["tail_last_change"] = time.time()
        for q in list(SSE_SUBS):
            try: q.put_nowait(REV["n"])
            except Exception: pass

def tail_loop():
    """进程内 tail：跟 daemon.log（0.4s），只读新增字节；轮转→重建；失败→可见轮询回退。"""
    ino = None; off = 0
    try:
        stt = os.stat(LOG); ino, off = stt.st_ino, stt.st_size   # 从 EOF 起（历史由启动 full_scan 覆盖）
    except OSError:
        ino = None
    fail = 0
    while True:
        try:
            stt = os.stat(LOG)
            if ino is not None and stt.st_ino != ino:
                full_scan(); ino, off = stt.st_ino, stt.st_size; bump()
            elif ino is None:
                ino, off = stt.st_ino, stt.st_size
            size = stt.st_size
            if size < off: off = 0
            if size > off:
                with open(LOG, "rb") as f:
                    f.seek(off); chunk = f.read(min(size - off, 2 << 20))
                off = size
                lines = chunk.decode("utf-8", errors="ignore").splitlines()
                if lines:
                    r = ingest_lines(lines)
                    HEALTH["tail_lines"] += r.get("accepted", 0)
                    bump()
            HEALTH.update({"tail_alive": True, "tail_mode": "tail",
                           "tail_last_read": time.time(), "tail_err": None})
            fail = 0
            time.sleep(0.4)
        except Exception as e:
            fail += 1
            HEALTH["tail_err"] = str(e)[:140]
            if fail >= 3:
                # 可见回退：节流 15s 的全量扫描，mode 标 poll —— 绝不静默冻结
                HEALTH["tail_alive"] = False; HEALTH["tail_mode"] = "poll"
                try:
                    if time.time() - HEALTH["_last_poll"] > 15:
                        full_scan(); HEALTH["_last_poll"] = time.time(); bump()
                except Exception as e2:
                    HEALTH["tail_err"] = ("poll:" + str(e2))[:140]
            time.sleep(1.0)

def sse_stream(handler):
    """SSE：变更即推 {rev}；每 15s 心跳保活；断连静默清理"""
    q = queue.Queue()
    with SSE_LOCK: SSE_SUBS.add(q)
    try:
        handler.wfile.write(b": connected\n\n"); handler.wfile.flush()
        while True:
            try:
                rev = q.get(timeout=15)
                handler.wfile.write(("data: {\"rev\": %d}\n\n" % rev).encode()); handler.wfile.flush()
            except queue.Empty:
                handler.wfile.write(b": hb\n\n"); handler.wfile.flush()
    except Exception:
        pass
    finally:
        with SSE_LOCK: SSE_SUBS.discard(q)

# ===== 产出物打开/下载：GET /api/file?path=…（Nova #230 GO / Sage #231 安全点）=====
# 安全模型：① 白名单 = 当前快照里出现过的产出物路径（精确串匹配）
#           ② resolve 后必须落在允许根内（containment；防目录穿越/越权读）
#           ③ 只读；④ html/svg/js/xml 等**可执行**类型一律 attachment（Sage：避免在
#              dashboard 源里被浏览器执行脚本 → 自造 SSRF/越权读）
ALLOW_ROOTS = [os.path.realpath(BASE), os.path.realpath(os.path.expanduser("~/.cumora"))]
IMAGE_PDF_EXT = {".pdf", ".png", ".jpg", ".jpeg", ".gif", ".webp"}
TEXT_EXT = {".txt", ".log", ".csv", ".md", ".py", ".json", ".yml", ".yaml", ".sh", ".go",
            ".rs", ".c", ".h", ".cpp", ".cc", ".ts", ".tsx", ".jsx", ".ini", ".toml",
            ".cfg", ".conf", ".sql", ".java", ".rb", ".php"}
ATTACH_EXT = {".html", ".htm", ".svg", ".js", ".mjs", ".xml"}   # 可执行 → 强制下载，不内联
MAX_FILE = 64 * 1024 * 1024

def _artifact_whitelist():
    """当前快照出现过的产出物路径（精确字符串集合）——唯一准入清单"""
    try:
        return set(a.get("path") for a in st.artifacts if a.get("path"))
    except Exception:
        return set()

def _resolve_dirs():
    """相对产出物的候选基准目录 —— 收紧（Nova #257/Sage 🔴）：
       候选目录**必须落在 ALLOW_ROOTS={BASE, ~/.cumora} 子树内**；不做无界递归、不涉及根外目录。
       仅取：BASE 本身 + BASE 一层子目录（非隐藏、非 __pycache__）+ ~/.cumora/agents/* 各工作区。"""
    cands = [BASE]
    try:
        cands += [os.path.join(BASE, d) for d in sorted(os.listdir(BASE))
                  if not d.startswith(".") and d != "__pycache__"]
    except Exception:
        pass
    try:
        adir = os.path.expanduser("~/.cumora/agents")
        cands += [os.path.join(adir, n) for n in sorted(os.listdir(adir))]
    except Exception:
        pass
    out = []
    for c in cands:
        rc = os.path.realpath(c)
        if os.path.isdir(rc) and _in_allow_roots(rc):
            out.append(rc)
    return out

def _in_allow_roots(rp):
    return any(rp == r or rp.startswith(r + os.sep) for r in ALLOW_ROOTS)

def _resolve_artifact(p):
    """白名单路径 → 存在的绝对路径（含允许根 containment 校验）；失败返回 None。
       相对名（如 `server.py`、`server-before-*.py`）会搜 BASE 一层子目录（含 _backup/）+ agent 工作区。"""
    if os.path.isabs(p):
        cands = [p]
    else:
        # 只在这些「根内、有界」候选目录里逐个试；**不做 os.walk 无界递归**（Nova #257）
        cands = [os.path.join(d, p) for d in _resolve_dirs()]
    for c in cands:
        rp = os.path.realpath(c)
        if os.path.isfile(rp) and _in_allow_roots(rp):
            return rp
    return None

def serve_file(handler):
    p = (parse_qs(urlparse(handler.path).query).get("path") or [""])[0]
    if not p or p not in _artifact_whitelist():
        return handler._send(403, "text/plain; charset=utf-8", b"forbidden: not a known artifact")
    rp = _resolve_artifact(p)
    if not rp:
        return handler._send(404, "text/plain; charset=utf-8", b"not found or outside allowed roots")
    ext = os.path.splitext(rp)[1].lower()
    if ext in ATTACH_EXT:
        ctype, disp = "application/octet-stream", "attachment"
    elif ext in TEXT_EXT:
        ctype, disp = "text/plain; charset=utf-8", "inline"   # 文本/代码：浏览器内查看
    elif ext in IMAGE_PDF_EXT:
        ctype, disp = (mimetypes.guess_type(rp)[0] or "application/octet-stream"), "inline"
    else:
        ctype, disp = (mimetypes.guess_type(rp)[0] or "application/octet-stream"), "attachment"
    try:
        sz = os.path.getsize(rp)
        if sz > MAX_FILE:
            return handler._send(413, "text/plain; charset=utf-8", b"file too large")
        with open(rp, "rb") as f:
            body = f.read()
    except Exception as e:
        return handler._send(500, "text/plain; charset=utf-8", str(e)[:200].encode())
    handler.send_response(200)
    handler.send_header("Content-Type", ctype)
    handler.send_header("Content-Disposition", '%s; filename="%s"' % (disp, os.path.basename(rp)))
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)

# ===== agents 元数据（BRAM #301，add-only）：从 `cumora participants` 取 id/role/status（含自身），
# 从 `cumora contacts` 补显示名 → 前端去硬编码（role 为真值）。TTL 缓存，无额外压力。
AGENTS_TTL = 30.0
_agents_cache = {"t": 0.0, "data": None}
PART_RE = re.compile(r'^\s*([A-Za-z0-9_-]+)\s+(agent|human)\s+(\S+)\s+(.*?)\s*$')

def load_agents():
    now = time.time()
    if _agents_cache["data"] is not None and now - _agents_cache["t"] < AGENTS_TTL:
        return _agents_cache["data"]
    names = {}
    try:   # contacts: kind name role address id（不含自身）
        out = subprocess.run(["cumora", "contacts", "--kind", "agent"],
                             capture_output=True, text=True, timeout=8).stdout
        for ln in (out or "").splitlines():
            parts = ln.split()
            if len(parts) >= 5 and parts[0] in ("agent", "human"):
                names[parts[-1]] = parts[1]
    except Exception:
        pass
    agents = []
    try:   # participants: id kind status role（含自身）
        out = subprocess.run(["cumora", "participants"],
                             capture_output=True, text=True, timeout=8).stdout
        for ln in (out or "").splitlines():
            if "avatar" in ln or ln.strip().startswith("↳"):
                continue
            m = PART_RE.match(ln)
            if not m or m.group(2) != "agent":
                continue
            full, _kind, status, role = m.group(1), m.group(2), m.group(3), m.group(4).strip()
            sid = ash(full)
            agents.append({"id": sid, "name": names.get(full, sid.capitalize()),
                           "role": role, "status": status})
    except Exception:
        pass
    order = {a: i for i, a in enumerate(AGENT_SHORT)}
    agents.sort(key=lambda a: order.get(a["id"], 99))
    _agents_cache["t"] = now
    _agents_cache["data"] = agents
    return agents

def snapshot():
    check_rotate()
    # BRAM:running-turn —— 只加不改：把「进行中」turn（st.turn）并入 tasks，
    # 使 running_tasks / agent_status / 前端 runningIds 能反映真实活动。
    # 现有字段名与结构不变（done 仍是仅已结束；tasks 仅新增运行中的条目）。
    tasks = sorted(list(st.tasks.values()) + list(st.turn.values()),
                   key=lambda x: x["start_ts"], reverse=True)
    running = [t for t in tasks if t["status"] == "running"]
    done = [t for t in tasks if t["status"] == "done"]
    rl = load_real()                                   # P0 换源：真实 Cumora 消息
    msgs = rl["messages"]                              # schema 不变：{t,a,g,txt,ts}
    # BRAM:#211 —— 产出物按 path 去重（保留最新 ts），一张卡一条线
    _ded = {}
    for a in st.artifacts:
        _p = a.get("path")
        if not _p:
            continue
        if _p not in _ded or a.get("ts", 0) > _ded[_p].get("ts", 0):
            _ded[_p] = a
    arts = [{"t": a["t"], "a": a["a"], "g": a["g"], "path": a["path"]}
            for a in sorted(_ded.values(), key=lambda x: x.get("ts", 0), reverse=True)[:200]]
    comm = sorted(rl["comm"], key=lambda x: x["ts"], reverse=True)[:10]
    boss = sorted(rl["boss_mentions"], key=lambda x: x["ts"], reverse=True)[:5]
    # 群 token 结构
    tok_g = {}
    for g, d in st.tok_g.items():
        ags = {a: {"i": t["i"], "o": t["o"], "r": t["r"], "steps": t["steps"]}
               for a, t in sorted(d.items())}
        tot = {"i": sum(x["i"] for x in d.values()), "o": sum(x["o"] for x in d.values()),
               "r": sum(x["r"] for x in d.values()), "steps": sum(x["steps"] for x in d.values())}
        tok_g[g] = {"agents": ags, "total": tot}
    tok_all = {a: {"i": t["i"], "o": t["o"], "r": t["r"], "steps": t["steps"]}
               for a, t in st.tok_all.items()}
    return {
        "generated_at": int(time.time() * 1000),
        "window_h": 4,
        # ★ 实际可见窗口（小时）＋ 条数上限状态 ⇒ 广告 4h 与实际给多少**都如实报**
        "msg_window_h": rl.get("msg_window_h", 0.0),
        "msg_tail_cap": rl.get("msg_tail_cap", REAL_TAIL),
        "msg_tail_hit": rl.get("msg_tail_hit", False),
        "running_tasks": len(running), "done_tasks": len(done),
        "tasks": tasks[:MAX_TASK],
        "messages": msgs,
        "artifacts": arts,
        "comm": comm,
        "boss_mentions": boss,
        "events": st.events[:MAX_EV],
        "groups": [{"id": g, "name": d["name"], "cnt": d["cnt"], "last_ts": int(d.get("last_ts", 0))}
                   for g, d in sorted(st.groups.items(), key=lambda kv: -kv[1]["cnt"])],
        "agents": load_agents(),   # BRAM #301 add-only：{id,name,role,status} 取自 cumora participants/contacts
        "tokens_g": tok_g,
        "tokens_all": tok_all,
        "all_total": dict(st.tok_total),
        "agent_status": {a: ("running" if a in {t["agent"] for t in running} else "idle")
                         for a in AGENT_SHORT},
        # Phase 0/1 健康面（只新增字段）：tail 存活/模式/心跳/错误 + SSE 订阅数
        "health": {"tail_alive": HEALTH["tail_alive"], "tail_mode": HEALTH["tail_mode"],
                   "tail_last_read": round(HEALTH["tail_last_read"], 3),
                   "tail_last_change": round(HEALTH["tail_last_change"], 3),
                   "tail_lines": HEALTH["tail_lines"], "tail_err": HEALTH["tail_err"],
                   "rev": REV["n"], "sse_clients": len(SSE_SUBS)},
    }

HTML = None
def get_html():
    global HTML
    if HTML is None:
        HTML = None
        for _name in HTML_CANDIDATES:            # 逐个候选：$CC_HTML → index.html → 中文名
            _p = os.path.join(BASE, _name)
            if os.path.isfile(_p):
                try:
                    HTML = open(_p, "r", encoding="utf-8").read()
                    break
                except Exception:
                    pass
        if HTML is None:
            HTML = "<h1>Cumora 指挥中心未找到</h1>"
    return HTML

class Handler(BaseHTTPRequestHandler):
    def _send(self, code, ctype, body):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/Cumora"):
            self._send(200, "text/html; charset=utf-8", get_html().encode("utf-8"))
        elif self.path.startswith("/api/file"):
            serve_file(self)
        elif self.path == "/api/stream":
            # Phase 1：SSE 变更推送（长连；变更/心跳帧）
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "keep-alive")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            sse_stream(self)
        elif self.path == "/api/snapshot":
            try:
                s = json.dumps(snapshot(), ensure_ascii=False)
                self._send(200, "application/json; charset=utf-8", s.encode("utf-8"))
            except Exception as e:
                self._send(500, "application/json; charset=utf-8",
                           json.dumps({"error": str(e)}).encode("utf-8"))
        else:
            self._send(404, "text/plain; charset=utf-8", b"not found")

    def do_POST(self):
        if self.path == "/api/ingest":
            try:
                n = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(n) if n else b"{}"
                req = json.loads(body.decode("utf-8", errors="ignore")) if body else {}
                if req.get("rotate"):
                    full_scan()   # pusher 通知轮转：重建后 pusher 从当前末尾续推
                    self._send(200, "application/json; charset=utf-8",
                               json.dumps({"rotated": True}).encode("utf-8"))
                    return
                lines = req.get("lines") or []
                r = ingest_lines(lines)
                self._send(200, "application/json; charset=utf-8",
                           json.dumps(r).encode("utf-8"))
            except Exception as e:
                self._send(500, "application/json; charset=utf-8",
                           json.dumps({"error": str(e)}).encode("utf-8"))
        else:
            self._send(404, "text/plain; charset=utf-8", b"not found")

    def log_message(self, *a):
        pass

if __name__ == "__main__":
    print("[cumora-cc] 首次全量扫描 daemon.log ...")
    t0 = time.time()
    try:
        full_scan()
        print("[cumora-cc] 扫描完成 %.1fs · %d 任务 / %d 消息 / %d 产出物" %
              (time.time() - t0, len(st.tasks), len(st.messages), len(st.artifacts)))
    except FileNotFoundError:
        print("[cumora-cc] 警告: 未找到 %s，等待日志出现" % LOG)
    # Phase 0：进程内 tail（不再依赖外部 pusher/手动重启）
    threading.Thread(target=tail_loop, daemon=True).start()
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print("[cumora-cc] 实时服务: http://127.0.0.1:%d/  (Ctrl+C 退出)" % PORT)
    print("[cumora-cc] Phase 0 tail 线程已启 · Phase 1 SSE: GET /api/stream · POST /api/ingest 兼容")
    srv.serve_forever()
