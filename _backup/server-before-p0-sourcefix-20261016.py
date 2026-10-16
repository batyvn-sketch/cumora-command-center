#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cumora 实时指挥中心 · 本地数据服务
- 解析 ~/.cumora/daemon.log（增量），提取：任务(turn 生命周期) / 产出物 / 消息全文 / 群归属 / token 用量
- 提供 GET /api/snapshot（最近 4 小时窗口 + token 全量累计），CORS 开放
- 托管同目录 Cumora指挥中心.html
用法: python3 server.py [port]   (默认 8787)
"""
import re, json, os, time, threading, sys, hashlib
from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, timezone

BASE = os.path.dirname(os.path.abspath(__file__))
LOG  = os.path.expanduser("~/.cumora/daemon.log")
LOGS = [LOG + ".1", LOG]   # 轮转后的旧主文件在前，当前文件在后
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8787
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
                self.messages.append({"t": hms, "ts": ts, "a": agent_s, "g": g, "txt": txt})
                self.events.append({"t": hms, "ts": ts, "a": agent_s, "e": "text",
                                    "m": txt[:90], "g": g, "full": txt})
                # 产出物：文本里的文件路径
                for p in PATH_RE.findall(txt) + REL_PATH_RE.findall(txt):
                    p = p.rstrip(".,;:)")
                    if len(p) > 6:
                        self.artifacts.append({"t": hms, "ts": ts, "a": agent_s, "g": g, "path": p})
                        if tk: tk["artifacts"].append(p)
                # agent 间沟通：@提及 / "Agent名 #序号" / "Agent名's"（sender 提到 target）
                seen = set()
                for m2 in re.finditer(r'@([A-Za-z0-9][A-Za-z0-9-]{1,19})', txt):
                    _raw = m2.group(1).lower()
                    if _raw == "you" or _raw.startswith("u-"):
                        if "you" not in seen:
                            seen.add("you")
                            self.comm.append({"from": agent_s, "to": "you", "t": hms, "ts": ts})
                        continue
                    tgt = ash(_raw)
                    if tgt and tgt != agent_s and tgt in AGENT_SHORT and tgt not in seen:
                        seen.add(tgt)
                        self.comm.append({"from": agent_s, "to": tgt, "t": hms, "ts": ts})
                for name in AGENT_SHORT:
                    if name == agent_s or name in seen:
                        continue
                    if re.search(r'(?<![a-z0-9])' + name + r'(\s#\d+|\x27s|\u2019s|\s)', txt, re.I):
                        self.comm.append({"from": agent_s, "to": name, "t": hms, "ts": ts})
                # 提到用户（BOSS）：@you / @u-xxx / batyvn
                if re.search(r'@you\b|@u-[a-f0-9]+|\bbatyvn\b', txt):
                    self.boss_mentions.append({"from": agent_s, "t": hms, "ts": ts})
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

def snapshot():
    check_rotate()
    tasks = sorted(st.tasks.values(), key=lambda x: x["start_ts"], reverse=True)
    running = [t for t in tasks if t["status"] == "running"]
    done = [t for t in tasks if t["status"] == "done"]
    msgs = [{"t": m["t"], "a": m["a"], "g": m["g"], "txt": m["txt"], "ts": m["ts"]} for m in st.messages[:MAX_MSG]]
    arts = [{"t": a["t"], "a": a["a"], "g": a["g"], "path": a["path"]} for a in st.artifacts[:200]]
    comm = sorted(st.comm, key=lambda x: x["ts"], reverse=True)[:10]
    boss = sorted(st.boss_mentions, key=lambda x: x["ts"], reverse=True)[:5]
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
        "running_tasks": len(running), "done_tasks": len(done),
        "tasks": tasks[:MAX_TASK],
        "messages": msgs,
        "artifacts": arts,
        "comm": comm,
        "boss_mentions": boss,
        "events": st.events[:MAX_EV],
        "groups": [{"id": g, "name": d["name"], "cnt": d["cnt"]}
                   for g, d in sorted(st.groups.items(), key=lambda kv: -kv[1]["cnt"])],
        "tokens_g": tok_g,
        "tokens_all": tok_all,
        "all_total": dict(st.tok_total),
        "agent_status": {a: ("running" if a in {t["agent"] for t in running} else "idle")
                         for a in AGENT_SHORT},
    }

HTML = None
def get_html():
    global HTML
    if HTML is None:
        p = os.path.join(BASE, "Cumora指挥中心.html")
        try:
            HTML = open(p, "r", encoding="utf-8").read()
        except Exception:
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
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print("[cumora-cc] 实时服务: http://127.0.0.1:%d/  (Ctrl+C 退出)" % PORT)
    print("[cumora-cc] 增量端点: POST /api/ingest (方案A pusher 推送) · 轮询兜底 5s")
    srv.serve_forever()
