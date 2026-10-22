#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""快照消息覆盖量探针（card-e9e8bdf4-c09 · 关闭证据的机器形态）

**判据**：**快照里的消息条数 ≥ 各活跃会话在广告窗口内的真实消息条数**
（广告窗口 = `window_h`；真实条数 = `cumora messages <cid> --tail 500 --json` 里落在窗口内的条数）
⇒ 覆盖率 < 100% ⇒ **判红**（读者看到的少 ⇒ 「消息数随说话变」这条自检会骗人）

**为什么是这个判据**：@sage-6hig 04:00Z 报的「发一条前后 messages=96 不变」**初判归因写成了
「tailer 丢旧消息」⇒ 实测是误归因**（tailer 只喂日志侧的事件/产出物；消息列表走
`cumora messages --tail REAL_TAIL`）⇒ **真截断点 = 每会话条数上限（`REAL_TAIL`）**
⇒ ⇒ 本探针量的就是那个上限造成的缺口 ⇒ 改上限（或改广告窗口）后转绿 = 关闭证据。

**用法**
    python3 tools/probe_snapshot_coverage.py [--url http://127.0.0.1:8787] [--window-h 4]
**退出码**：0 = 覆盖 100%（PASS）｜1 = 有缺口（FAIL）｜2 = 未对账（读不到快照/读不到消息 ⇒ 不许当绿）
"""
import argparse
import datetime
import json
import subprocess
import sys
import urllib.request

DEFAULT_URL = "http://127.0.0.1:8787"


def fetch_snapshot(url):
    with urllib.request.urlopen(url.rstrip("/") + "/api/snapshot", timeout=20) as r:
        return json.load(r)


def real_counts(convos, cutoff):
    """每个会话在窗口内的真实消息条数（用 CLI 结构化真身，不猜）"""
    out = {}
    for cid in convos:
        try:
            raw = subprocess.run(["cumora", "messages", cid, "--tail", "500", "--json"],
                                 capture_output=True, text=True, timeout=90).stdout
            data = json.loads(raw)
        except Exception:
            out[cid] = None          # 读不到 ⇒ 未对账
            continue
        n = 0
        for m in data:
            ca = m.get("created_at") or ""
            try:
                ts = datetime.datetime.fromisoformat(ca.replace("Z", "+00:00"))
            except Exception:
                continue
            if ts >= cutoff:
                n += 1
        out[cid] = n
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description="快照消息覆盖量探针（card-e9e8bdf4-c09）")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--window-h", type=float, default=None, help="默认取快照里的 window_h")
    args = ap.parse_args(argv)

    try:
        snap = fetch_snapshot(args.url)
    except Exception as e:
        print(f"⛔ 快照读不到（{e}）⇒ 未对账（不许当通过）")
        return 2
    win = args.window_h if args.window_h is not None else (snap.get("window_h") or 4)
    now = datetime.datetime.now(datetime.timezone.utc)
    cutoff = now - datetime.timedelta(hours=win)

    msgs = snap.get("messages") or []
    # 快照里的群 id 是**短名**（emag / priv / eda）或真 id（g-e24ceb6d）
    #   ⇒ 靠 `cumora conversations --json` 的「群名 → 会话 id」换回去
    #   ⚠ 换不回去 ⇒ **未对账**（不许当通过）—— 我第一版没换 ⇒ 测到「真实 0 条」还报 PASS = 假绿
    name2id = {}
    try:
        for c in json.loads(subprocess.run(["cumora", "conversations", "--json"],
                                          capture_output=True, text=True, timeout=60).stdout):
            if c.get("kind") == "group" and c.get("title"):
                name2id[c["title"]] = c["id"]
    except Exception:
        pass
    alias = {}
    skip = {}
    win_start = cutoff.timestamp()
    for g in (snap.get("groups") or []):
        gid = g.get("id")
        gname = g.get("name")
        if gid and gid.startswith("g-"):
            alias[gid] = gid
        elif gid and gname and gname in name2id:
            alias[gid] = name2id[gname]
        else:
            # 换不回去的两种情况，都不是「少读了」：
            # ① 'priv' = 私聊聚合名（不是会话 id ⇒ 真实条数无从取得 ⇒ 不参与比较）
            # ② 窗口内没有活动的群（last_ts < 窗口起点 ⇒ 真实条数恒 0 ⇒ 不参与比较）
            if gid == "priv":
                skip[gid] = "私聊聚合名（不是会话 id）"
            elif (g.get("last_ts") or 0) < win_start:
                skip[gid] = "窗口内无活动（last_ts 早于窗口起点）"
            else:
                skip[gid] = "**换不回去会话 id ⇒ 未对账**"
    convos = []
    for m in msgs:
        g = m.get("g")
        if g and g not in convos:
            convos.append(g)
    real_ids = [alias.get(g) for g in convos]
    for g in convos:
        if g not in alias:
            real_ids[convos.index(g)] = None

    print("=" * 78)
    print("快照消息覆盖量探针（card-e9e8bdf4-c09 · 纪律㉞-171 覆盖量形态）")
    print("=" * 78)
    print(f"  快照 = {args.url} ｜ 广告窗口 = {win}h ｜ 窗口起点 = {cutoff.strftime('%H:%M')}Z")
    print(f"  快照 messages = {len(msgs)} 条")

    counts = real_counts(real_ids, cutoff)
    total_real = 0
    unreadable = []
    per = {}
    for short, cid in zip(convos, real_ids):
        if cid is None:
            why = skip.get(short, "换不回去会话 id")
            if "未对账" in why:
                unreadable.append(short)
                per[short] = None
            else:
                print(f"  · {short:<14} 不参与比较（{why}）")
                per[short] = 0
            continue
        n = counts.get(cid)
        if n is None:
            unreadable.append(short)
            per[short] = None
            continue
        per[short] = n
        total_real += n

    snap_by_g = {}
    for m in msgs:
        snap_by_g[m.get("g")] = snap_by_g.get(m.get("g"), 0) + 1
    for short in per:
        n_real = per[short]
        n_snap = snap_by_g.get(short, 0)
        if n_real is None:
            print(f"  · {short:<14} 真实条数 **读不到** ⇒ 未对账")
            continue
        ratio = (n_snap / n_real) if n_real else 1.0
        flag = "✔" if n_snap >= n_real else "✘"
        print(f"  {flag} {short:<14} 快照 {n_snap:>3} 条 / 真实 {n_real:>3} 条 = {ratio*100:6.1f}%")

    print("-" * 78)
    if unreadable:
        print(f"  覆盖量 = 扫过 {len(msgs)} 行（快照侧）｜真实侧 **读不到 {len(unreadable)} 个会话** ⇒ 未对账")
        print("⇒ 结论：**未对账**（有会话读不到真实条数 ⇒ 不许当通过）")
        return 2
    print(f"  覆盖量 = 扫过 {len(msgs)} 行（快照侧）｜真实侧合计 {total_real} 行"
          f"｜覆盖 {len(msgs)/total_real*100 if total_real else 100:.1f}%")
    if len(msgs) < total_real:
        print(f"⇒ 结论：**FAIL**（快照少了 {total_real - len(msgs)} 条 ⇒ 可见窗口短于广告的 {win}h"
              f" ⇒ 读者看不到旧消息 ⇒ 「消息数随说话变」那条自检会骗人）")
        return 1
    print(f"⇒ 结论：**PASS**（快照 {len(msgs)} 条 ≥ 真实 {total_real} 条 ⇒ 窗口内一条不丢）")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
