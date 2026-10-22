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
# ⇒ 再加**启动参数**（@nova-94d8 05:0xZ 裁「环境变量 / 启动参数」两路都要）：
#     `python3 server.py 8787 --tail 500` ⇒ 启动参数优先于环境变量。
def _tail_from_argv():
    for _i, _a in enumerate(sys.argv):
        if _a == "--tail" and _i + 1 < len(sys.argv):
            try:
                return int(sys.argv[_i + 1])
            except ValueError:
                pass
        if _a.startswith("--tail="):
            try:
                return int(_a.split("=", 1)[1])
            except ValueError:
                pass
    return None


REAL_TAIL = int(_tail_from_argv() or os.environ.get("CC_MSG_TAIL", "500"))  # 每会话取最近 N 条
# ★ 2026-10-22 Bram（card-e9e8bdf4-c09 · @nova-94d8 07:03Z 裁「提速」· 验收线 < 2 s）：
#   **慢在 CLI 上，不在本服务**：实测一次 `cumora messages <会话> --tail 500` ≈ 88 s
#   （成本 = 全量解析 20 MB daemon.log，与 --tail 的大小无关 ⇒ --tail 100 也是 88 s）
#   ⇒ 旧实现把这次拉取放在**请求路径**上 ⇒ 端点实测 36.41 s / 14.66 s ⇒ 验收线不过。
#   ⇒ 改法（三条同时做，缺一条都还会慢）：
#     ① **后台线程刷新缓存**（real_loop）⇒ 请求路径只读内存缓存 ⇒ 毫秒级返回；
#     ② **上次好数据落盘** ⇒ 冷启动首屏即有内容（不空等一次 88 s）；
#     ③ **拉取失败/超时保留好数据**（绝不拿空覆盖好数据）⇒ 并如实报 msg_fetch_err。
#   ⇒ 超时给到 180 s（实测 88 s 且随日志增长 ⇒ 25 s 必然超时 ⇒ 静默回空 = 静默骗人）。
REAL_FETCH_TIMEOUT = int(os.environ.get("CC_FETCH_TIMEOUT", "180"))
# 上次好数据的落盘位置（冷启动读取；不落进仓内，避免污染 git）
CACHE_FILE = os.path.join(os.path.expanduser("~/.cache"), "cumora-command-center", "msg_cache.json")
# 超过这个秒数没刷新成功 ⇒ 对外标「数据陈旧」（面板如实显示，不装作新鲜）
STALE_AFTER_S = float(os.environ.get("CC_STALE_AFTER", "180"))
_real_cache = {"t": 0.0, "data": None, "fetched_at": 0.0, "fetch_ms": 0,
               "err": None, "refreshing": False}
_real_lock = threading.Lock()
# 成功拉过的会话 id（回退清单用；避免「清单接口失败 ⇒ 一个群都不拉」）
_seen_convo_ids = set()
# 一轮最多拉多少个会话（每个会话一次 CLI 调用 ⇒ 太多会拖长一轮）
MAX_FETCH_CONVOS = int(os.environ.get("CC_MAX_CONVOS", "12"))
MSG_LINE_RE = re.compile(r'^\s*\[(m-[0-9a-f-]+)\]\s*\[\s*(\d{1,2}):(\d{2})\s*(AM|PM)\]\s+([A-Za-z0-9_-]+)\s*#\s*(\d+)\s+(.*)$')

def _active_convos():
    """窗口内有活动的会话（**真源 = `cumora conversations --json`**）。

    ★ 2026-10-22 Bram（card-e9e8bdf4-c09 · 提速改造）：**旧实现是坏的** ——
      它扫 daemon.log 尾部找 `for g-xxx` 的字样 ⇒ 实测全量 20 MB 日志里
      「for g-e24ceb6d」**0 命中**、「for g-ec7538ec」**2 命中**（还是老群）
      ⇒ 于是它只去拉那个早就没人说话的群 ⇒ **面板显示 0 条消息**
      ⇒ 「打开一次数据要等 25–41 秒 + 打开是空的」＝ 两个病叠在一起。
      ⇒ 改用 CLI 的会话清单（`cumora conversations --json` · 实测 0.55 s）
        ⇒ 按 `updated_at` 取窗口内有活动的会话（群 + 私聊，按活动排序取前 N 个）。
      ⇒ CLI 失败 ⇒ 回退到「已知群 + 上次缓存里的会话」（绝不静默返回空清单）。

    返回 (会话 id 列表, 来源标签) ⇒ 来源标签进快照（如 "cli" / "fallback"）便于排障。
    """
    try:
        r = subprocess.run(["cumora", "conversations", "--json"],
                           capture_output=True, text=True, timeout=REAL_FETCH_TIMEOUT)
        items = json.loads(r.stdout or "[]")
        cutoff = time.time() - WINDOW_H
        scored = []
        for it in items:
            _cid = str(it.get("id") or "")
            if not _cid:
                continue
            _upd = str(it.get("updated_at") or "")
            _ts = 0.0
            if _upd:
                try:
                    _ts = datetime.fromisoformat(_upd.replace("Z", "+00:00")).timestamp()
                except Exception:
                    _ts = 0.0
            if _ts >= cutoff:
                scored.append((_ts, _cid))
        scored.sort(reverse=True)
        _ids = [c for (_t, c) in scored[:MAX_FETCH_CONVOS]]
        if _ids:
            return _ids, "cli"
    except Exception:
        pass
    # 回退：已知群 + 以前成功拉过的会话（宁可多拉几个，也不要拉成空）
    _ids = list(CONVO_FALLBACK)
    try:
        with _real_lock:
            for _cid in sorted(_seen_convo_ids):
                if _cid not in _ids:
                    _ids.append(_cid)
    except Exception:
        pass
    return _ids[:MAX_FETCH_CONVOS], "fallback"

def _fetch_convo(cid):
    """拉单个会话的最近消息文本（CLI 结构化真身）；任何失败都返回空串，不抛。

    返回 (cid, 文本, 拉到的行数) ⇒ 行数用于判**条数上限有没有顶到**
    （顶到 = 这个会话的可见窗口被上限挤短 ⇒ 要如实报出来，不能继续宣称 4h）。

    ★ 2026-10-22 Bram：`--tail` 取 **REAL_TAIL 与「计数用的大 tail」的较大值**
      ⇒ **一次调用同时拿到「要显示的」与「真实条数」** ⇒ 省掉第二次 CLI 调用
      （旧实现：显示用 tail=500 一次 + 计数用 tail=2000 一次 ⇒ 被截断时会话要拉两遍，
        每遍 ~88 s ⇒ 一个会话 ~176 s）。
    """
    try:
        # ★ 超时给足：CLI 扫日志在冷启动/大日志时可能 88 s+（实测）⇒ 10/25 s 会**静默回空**
        #   ⇒ 空列表 = 「这个群没消息」⇒ 又是静默骗人。
        _tail = max(REAL_TAIL, REAL_TAIL * 4)
        r = subprocess.run(["cumora", "messages", cid, "--tail", str(_tail)],
                           capture_output=True, text=True, timeout=REAL_FETCH_TIMEOUT)
        _txt = r.stdout or ""
        return cid, _txt, len(_txt.splitlines())
    except Exception:
        return cid, "", 0


def _build_real_data(outs, now):
    """把各会话拉到的原始文本解析成快照数据（**纯函数**：不碰缓存、不做 IO）。

    ★ 2026-10-22 Bram（card-e9e8bdf4-c09 · 提速改造）：
      ① 拉取与解析分开 ⇒ 解析可以单测；② 「显示条数」与「真实条数」分开算：
         `shown` = 本会话**实际显示**的条数（受每会话上限 REAL_TAIL 约束）
         `real`  = 本会话在 4h 窗口内**真实存在**的条数（本次拉到的全部窗口内消息）
         ⇒ `truncated = (real > shown)` ⇒ 语义自洽：截断时恒有 shown < real
           （@nova-94d8 05:1xZ 裁的三字段语义 · 探针按这条判）
      ③ 「这次拉取顶到了 tail 上限」（可能还有更早的消息）是**另一件事**
         ⇒ 单列 `fetch_cap_hit` 字段，不混进 truncated（两个含义不许混）。

    时间戳只有时钟无日期：按同群消息时序从新到旧单调回推（时钟变大=跨天回退一天）重建。
    """
    gm = time.gmtime(now)
    midnight = calendar.timegm((gm.tm_year, gm.tm_mon, gm.tm_mday, 0, 0, 0, 0, 0, 0))
    now_clock = now - midnight
    # 条数上限有没有顶到（顶到 ⇒ 该会话的可见窗口被上限挤短 ⇒ 如实报，不装作 4h）
    _tail_hit = any(n >= REAL_TAIL for (_c, _t, n) in outs)

    msgs, comm, boss = [], [], []
    _per_convo = {}          # 会话 → 该会话窗口内的消息（按时间升序）
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
        _inwin = []
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
            _inwin.append({"t": hm, "a": a, "g": g, "txt": txt, "ts": ts, "sys": _sys})
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
        _inwin.sort(key=lambda x: x["ts"])
        _per_convo[cid] = _inwin
        # ★ 每会话显示条数上限（REAL_TAIL）⇒ 只显示最近 N 条，更早的只计入 real
        for _m in _inwin[-REAL_TAIL:]:
            _m["_cid"] = cid          # 记下会话 id（多个私聊都叫 priv ⇒ 汇总要按 id 分）
            msgs.append(_m)
    msgs.sort(key=lambda x: x["ts"])
    # ★ 全局上限 MAX_MSG：砍掉最早的那些 ⇒ 逐会话 shown 必须按**最终留下的**数算
    msgs = msgs[-MAX_MSG:]
    # 实际覆盖时长（窗口内最早一条到现在）⇒ 前端据此显示真实窗口，别只宣称 window_h
    _span_h = 0.0
    if msgs:
        _span_h = max(0.0, (now - min(m["ts"] for m in msgs)) / 3600.0)
    # ★ 逐会话的截断标记（@nova-94d8 05:1xZ 裁：truncated / real / shown）
    #   · shown = 本会话**实际显示**的条数｜real = 本会话窗口内真实条数
    #   · covered_h = 本会话能回看到多远（小时）⇒ 窗口按**最差的那个**对外承诺
    #   ⚠ 私聊的多个会话都叫「priv」⇒ 汇总层必须**按会话 id 累加**，
    #     否则 6 个私聊会互相覆盖（顶层 real 就会少算 ⇒ 三字段语义不自洽）。
    _by_id = {}
    for _cid, _out, _nlines in outs:
        _inwin = _per_convo.get(_cid) or []
        _real = len(_inwin)                                   # 窗口内真实条数
        _disp = _inwin[-REAL_TAIL:]                           # 每会话显示上限
        _by_id[_cid] = {"_disp": _disp, "real": _real,
                        "covered_h": round(max(0.0, (now - min((m["ts"] for m in _disp),
                                                                default=now)) / 3600.0), 3),
                        "fetched": _nlines,
                        "fetch_cap_hit": bool(_nlines >= max(REAL_TAIL * 4, 2000))}
    # 全局上限 MAX_MSG 会砍掉一部分 ⇒ 逐会话 shown 必须按**最终留下的**数来算
    _kept = {}
    for _m in msgs:
        _kept[_m["_cid"]] = _kept.get(_m["_cid"], 0) + 1
    for _cid, _c in _by_id.items():
        _shown = int(_kept.get(_cid, 0))
        _c["shown"] = _shown
        _c["truncated"] = bool(_c["real"] > _shown)
        _c.pop("_disp", None)
    # 按短名（群 = g-xxx 映射；私聊 = priv）汇总 ⇒ 面板逐会话显示
    _convos = {}
    for _cid, _c in _by_id.items():
        _g = gshort(_cid)
        _prev = _convos.get(_g)
        if _prev is None:
            _convos[_g] = dict(_c)
            _convos[_g]["truncated"] = bool(_c["truncated"])
        else:
            _prev["real"] += _c["real"]
            _prev["shown"] += _c["shown"]
            _prev["truncated"] = bool(_prev["truncated"] or _c["truncated"])
            _prev["covered_h"] = min(_prev["covered_h"], _c["covered_h"]) if _prev["shown"] else _c["covered_h"]
            _prev["fetch_cap_hit"] = bool(_prev["fetch_cap_hit"] or _c["fetch_cap_hit"])
    # ★ 顶层三字段按**会话 id 累加**（与 msgs 同一口径 ⇒ 语义自洽：
    #   truncated=True ⇒ shown < real；truncated=False ⇒ shown >= real）
    _real_total = sum(c["real"] for c in _by_id.values())
    # 全局上限 MAX_MSG 砍掉的那部分也算「被截断」⇒ 否则 shown < real 而 truncated=False
    _trunc_total = bool(any(c["truncated"] for c in _by_id.values())
                         or _real_total > len(msgs))
    _covs = [c["covered_h"] for c in _convos.values() if c["shown"] > 0]
    _promise_h = round(min(_covs), 3) if _covs else 0.0
    return {"messages": msgs, "comm": comm, "boss_mentions": boss,
            "msg_window_h": round(_span_h, 3), "msg_tail_cap": REAL_TAIL,
            "msg_tail_hit": bool(_tail_hit), "msg_convos": _convos,
            "msg_window_promise_h": _promise_h,
            "msg_real_total": _real_total, "msg_trunc_total": bool(_trunc_total),
            "msg_convos_by_id": _by_id}


def _save_cache_to_disk(data):
    """把「上次好数据」落盘 ⇒ 冷启动首屏即有内容（不空等一次 ~88 s 的 CLI）。"""
    try:
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        with open(CACHE_FILE, "w", encoding="utf-8") as fh:
            json.dump({"saved_at": time.time(), "data": data}, fh, ensure_ascii=False)
    except Exception:
        pass          # 落盘失败不影响服务（下次启动仍是空首屏 ⇒ 但不会崩）


def _load_cache_from_disk():
    """读「上次好数据」⇒ 返回 (data, saved_at)；没有或坏文件 ⇒ (None, 0)。"""
    try:
        with open(CACHE_FILE, encoding="utf-8") as fh:
            obj = json.load(fh)
        return obj.get("data"), float(obj.get("saved_at") or 0.0)
    except Exception:
        return None, 0.0


def _refresh_real():
    """后台刷新一次真消息缓存（**只在后台线程里调用**）。

    ★ 失败/超时 ⇒ **保留上次好数据**（绝不拿空覆盖好数据）⇒ 并如实记 msg_fetch_err。
      （旧实现：拉取失败 ⇒ 空列表 ⇒ 面板显示「这个群没消息」⇒ 静默骗人。）
    """
    with _real_lock:
        if _real_cache["refreshing"]:
            return False
        _real_cache["refreshing"] = True
    t0 = time.time()
    try:
        convos, _src = _active_convos()
        try:
            with ThreadPoolExecutor(max_workers=max(1, len(convos))) as ex:
                outs = list(ex.map(_fetch_convo, convos))
        except Exception:
            outs = [_fetch_convo(c) for c in convos]
        data = _build_real_data(outs, time.time())
        if not data.get("messages"):
            # 一条都没拉到 ⇒ 算失败 ⇒ 保留上次好数据（面板标 stale + err，不装作「群里没消息」）
            raise RuntimeError("本次拉取 0 条消息（保留上次好数据 · 会话清单=%s）" % (convos,))
        _done = time.time()
        with _real_lock:
            _real_cache.update({"t": _done, "data": data, "fetched_at": _done,
                                "fetch_ms": int((_done - t0) * 1000), "err": None,
                                "convo_src": _src, "convo_n": len(convos)})
        for _cid, _txt, _n in outs:
            if _n > 0:
                _seen_convo_ids.add(_cid)
        _save_cache_to_disk(data)
        bump()
        return True
    except Exception as e:
        with _real_lock:
            _real_cache["err"] = str(e)[:200]
        return False
    finally:
        with _real_lock:
            _real_cache["refreshing"] = False


def real_loop():
    """后台刷新线程：周期性拉真消息 ⇒ 请求路径永远只读缓存（毫秒级）。

    ★ 周期 = 拉取耗时 + REAL_TTL（拉一次 ~88 s ⇒ 实际刷新周期 ~100 s）
      ⇒ 面板上用 msg_age_s 如实告诉读者「这份数据是多久前的」⇒ 不装作新鲜。
    ★ 顺带刷新 agent 名单（也是 CLI 调用 ⇒ 同样不许放请求路径）。
    """
    while True:
        try:
            _refresh_agents()
        except Exception:
            pass
        try:
            _refresh_real()
        except Exception:
            pass
        time.sleep(REAL_TTL)


def load_real():
    """取真消息快照（**前台直读缓存 · 绝不阻塞**）。

    ★ 2026-10-22 Bram（card-e9e8bdf4-c09 · @nova-94d8 07:03Z 裁「提速」· 验收线 < 2 s）：
      旧实现把 CLI 拉取放在**请求路径**上（实测 36.41 s / 14.66 s ⇒ 不过线）
      ⇒ 现在：**后台线程**刷新（real_loop）· 前台只读内存缓存 ⇒ 毫秒级。
      冷启动 ⇒ 先读磁盘上的「上次好数据」（首屏即有内容）。
    """
    with _real_lock:
        data = _real_cache["data"]
    if data is None:
        data, saved_at = _load_cache_from_disk()
        if data is not None:
            with _real_lock:
                if _real_cache["data"] is None:
                    _real_cache["data"] = data
                    _real_cache["fetched_at"] = saved_at      # 用落盘时刻算 age（不是现在）
    return data or {"messages": [], "comm": [], "boss_mentions": [],
                    "msg_window_h": 0.0, "msg_tail_cap": REAL_TAIL,
                    "msg_tail_hit": False, "msg_convos": {},
                    "msg_window_promise_h": 0.0}

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

def _refresh_agents():
    """拉一次 agent 名单（**只在后台线程里调用**）：两个 CLI 调用，冷启动可能十几秒。

    ★ 2026-10-22 Bram（card-e9e8bdf4-c09 · 提速）：旧实现把这两个调用放在**请求路径**上
      ⇒ 冷启动后第一次 /api/snapshot 实测 13.4 s ⇒ 验收线（< 2 s）不过
      ⇒ 改成「后台拉 + 前台只读缓存」。
    """
    now = time.time()
    names = {}
    try:   # contacts: kind name role address id（不含自身）
        out = subprocess.run(["cumora", "contacts", "--kind", "agent"],
                             capture_output=True, text=True, timeout=REAL_FETCH_TIMEOUT).stdout
        for ln in (out or "").splitlines():
            parts = ln.split()
            if len(parts) >= 5 and parts[0] in ("agent", "human"):
                names[parts[-1]] = parts[1]
    except Exception:
        pass
    agents = []
    try:   # participants: id kind status role（含自身）
        out = subprocess.run(["cumora", "participants"],
                             capture_output=True, text=True, timeout=REAL_FETCH_TIMEOUT).stdout
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
    _agents_cache["t"] = time.time()      # ★ 记**完成时刻**（不是开始时刻）
    _agents_cache["data"] = agents
    return agents


def load_agents():
    """agent 名单（**前台直读缓存 · 绝不阻塞**）；没有缓存 ⇒ 触发一次后台拉取并先返回空。"""
    data = _agents_cache.get("data")
    if data is None:
        threading.Thread(target=_refresh_agents, daemon=True).start()
        return []
    return data

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
    # ★ 新鲜度如实上报（card-e9e8bdf4-c09 · 提速改造）：
    #   拉取在后台（~88 s/次）⇒ 前端拿到的可能是「几十秒前」的数据
    #   ⇒ 必须让读者知道（age / stale / refreshing / 上次耗时 / 上次错误）
    #   ⇒ 不许拿旧数据装作新鲜（纪律㉞-181：不许静默）。
    with _real_lock:
        _fetched_at = float(_real_cache.get("fetched_at") or 0.0)
        _fetch_ms = int(_real_cache.get("fetch_ms") or 0)
        _fetch_err = _real_cache.get("err")
        _refreshing = bool(_real_cache.get("refreshing"))
    _age_s = round(max(0.0, time.time() - _fetched_at), 1) if _fetched_at else None
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
        # ★ 对外写的窗口 = **实际能覆盖的范围**（@nova-94d8 05:0xZ 裁「先承诺改准」）
        #   ⇒ 取各会话覆盖时长的**最小值**（我们对每个群都敢承诺的那个窗口）
        #   ⇒ 另给 msg_window_h（最宽能回看到多远）· msg_convos（逐会话 truncated/real/shown）
        "window_h": rl.get("msg_window_promise_h", 0.0) or 4,
        "msg_window_h": rl.get("msg_window_h", 0.0),
        "msg_window_promise_h": rl.get("msg_window_promise_h", 0.0),
        "msg_tail_cap": rl.get("msg_tail_cap", REAL_TAIL),
        "msg_tail_hit": rl.get("msg_tail_hit", False),
        "msg_convos": rl.get("msg_convos", {}),
        # ★ 截断标记三字段（@nova-94d8 05:0xZ 裁的**字段名** · 面板按这三字段显示
        #   「已截断 · 显示 shown / 真实 real」）⇒ 顶层给汇总，逐会话明细在 msg_convos。
        #   语义：truncated=True ⇒ shown < real；truncated=False ⇒ shown >= real。
        "truncated": bool(rl.get("msg_trunc_total", rl.get("msg_tail_hit", False))),
        "real": int(rl.get("msg_real_total", 0)),
        "shown": len(msgs),
        # ★ **每会话一组** truncated/real/shown（@nova-94d8 05:1xZ 裁）
        "convs": {g: {"truncated": bool(c.get("truncated")),
                      "real": int(c.get("real", 0)),
                      "shown": int(c.get("shown", 0)),
                      "covered_h": c.get("covered_h", 0.0),
                      "fetch_cap_hit": bool(c.get("fetch_cap_hit", False))}
                  for g, c in (rl.get("msg_convos") or {}).items()},
        # ★ 新鲜度（拉取在后台 ⇒ 如实说这份数据是多久前的）
        "msg_fetched_at": int(_fetched_at) if _fetched_at else 0,
        "msg_age_s": _age_s,
        "msg_stale": bool(_age_s is None or _age_s > STALE_AFTER_S),
        "msg_refreshing": _refreshing,
        "msg_fetch_ms": _fetch_ms,
        "msg_fetch_err": _fetch_err,
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
        # ★ 服务抬头印「本次取 N 条」（@nova-94d8 05:0xZ 裁「甲」）
        #   ⇒ 读者/抓包者一眼知道这次拉了多少条 ⇒ 截断不再静默。
        self.send_header("X-Cumora-Msg-Tail", str(REAL_TAIL))
        #   （HTTP 头只能 latin-1 ⇒ 中文注释放进 snapshot 的 msg_tail_cap / 面板显示）
        self.send_header("X-Cumora-Msg-Tail-Note",
                         "msg tail N=%d per session" % REAL_TAIL)
        # ★ 新鲜度（拉取在后台 ⇒ 读者/抓包者能直接看到这份数据是多久前的）
        with _real_lock:
            _fa = float(_real_cache.get("fetched_at") or 0.0)
        self.send_header("X-Cumora-Msg-Age",
                         ("%.1f" % (time.time() - _fa)) if _fa else "unknown")
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
    # ★ 2026-10-22 Bram（card-e9e8bdf4-c09 · 提速）：真消息拉取放**后台**线程
    #   （一次 ~88 s ⇒ 不能放请求路径）⇒ 冷启动先读磁盘上的「上次好数据」⇒ 首屏即有内容。
    _cached, _saved_at = _load_cache_from_disk()
    if _cached is not None:
        with _real_lock:
            _real_cache["data"] = _cached
            _real_cache["fetched_at"] = _saved_at
        print("[cumora-cc] 真消息缓存：读上次好数据（%d 条 · %s 前）"
              % (len(_cached.get("messages") or []),
                 ("%.0f s" % (time.time() - _saved_at)) if _saved_at else "?"))
    threading.Thread(target=real_loop, daemon=True).start()
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print("[cumora-cc] 实时服务: http://127.0.0.1:%d/  (Ctrl+C 退出)" % PORT)
    print("[cumora-cc] Phase 0 tail 线程已启 · Phase 1 SSE: GET /api/stream · POST /api/ingest 兼容")
    print("[cumora-cc] 真消息后台刷新线程已启（拉取在后台 ⇒ /api/snapshot 毫秒级返回）")
    srv.serve_forever()
