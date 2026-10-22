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
import time
import urllib.request

DEFAULT_URL = "http://127.0.0.1:8787"


# ★ 2026-10-22 修（@sage-6hig · 纪律㉞-171 补句「rc 的语义」）：
#   **timeout 从 20 s 提到 90 s** —— 端点当前实测耗时 16–41 s（@nova-94d8 06:2xZ 测得 25.07/41.11 s）
#   ⇒ 20 s 的 timeout 让 rc 在**「读到了」与「没读到」之间随机翻**（实测：同一份代码 1 次 rc=0 + 3 次 rc=2）
#   ⇒ ⇒ **rc 不能表达「这次覆盖量合不合格」** ⇒ 违反 PM 06:4xZ 立的规矩
#      （rc 只表达「本次执行是否符合【这次的预期】」⇒ 预期不同 ⇒ rc 的含义不同）。
#   ⇒ 修法：**rc 只管覆盖量语义**（timeout 放宽到 90 s）；**端点耗时单独一行读数 + 单独一个判定**
#      （PM 的提速验收 = 同一时刻连打两次都 < 2 s ⇒ 归 @bram-500f）⇒ 不再用 timeout 当隐形的第三态。
SNAP_TIMEOUT = 90
LATENCY_LIMIT_S = 2.0      # PM 的提速验收线（同一时刻连打两次都 < 2 s）


def fetch_snapshot(url, timeout=SNAP_TIMEOUT):
    t0 = time.time()
    with urllib.request.urlopen(url.rstrip("/") + "/api/snapshot", timeout=timeout) as r:
        data = json.load(r)
    return data, time.time() - t0


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

    # ── 端点耗时：**单独读数 + 单独判定**（PM 的提速验收：同一时刻连打两次都 < 2 s）
    #   ⇒ 连打两次 ⇒ 取**最坏那次**（不许只报好看的）
    lat = []
    snap = None
    for i in range(2):
        try:
            snap, dt = fetch_snapshot(args.url)
            lat.append(dt)
            print(f"  · 快照取回 #{i+1} 耗时 {dt:.2f} s")
        except Exception as e:
            print(f"⛔ 快照读不到（{e}）⇒ 未对账（不许当通过）")
            return 2
    worst = max(lat)
    print(f"  · 端点耗时（两次取最坏）= {worst:.2f} s ｜ PM 的提速验收线 = < {LATENCY_LIMIT_S:.0f} s")
    latency_ok = worst < LATENCY_LIMIT_S
    if latency_ok:
        print(f"  ✔ 提速验收：{worst:.2f} s < {LATENCY_LIMIT_S:.0f} s ✔")
    else:
        print(f"  ⚠ 提速验收**未过**：{worst:.2f} s ≥ {LATENCY_LIMIT_S:.0f} s"
              f" ⇒ 归 @bram-500f（每会话各拉一次 tail 500）⇒ **这条不并入 rc**（rc 只表达覆盖量语义）")
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

    # ---- 关闭证据 ②（@nova-94d8 05:0xZ 裁）：快照必须带 truncated / real / shown 三字段，
    #      且语义自洽（truncated=True ⇒ shown < real；truncated=False ⇒ shown >= real）
    print("-" * 78)
    print("关闭证据 ②（截断标记三字段 · @nova-94d8 05:0xZ 裁的字段名）")
    need = ("truncated", "real", "shown")
    missing = [k for k in need if k not in snap]
    if missing:
        print(f"  ✘ 缺字段：{' / '.join(missing)} ⇒ 快照没带截断标记 ⇒ 面板无法显示「已截断 · 显示 N / 真实 M」")
        near = {k: snap[k] for k in ("msg_tail_hit", "msg_tail_cap", "msg_window_h") if k in snap}
        if near:
            print(f"     现有近似字段（**字段名要按 PM 裁统一** · 纪律㉞-134/140 同一件事一处权威）：{near}")
    else:
        trunc = snap.get("truncated")
        real_n = snap.get("real")
        shown_n = snap.get("shown")
        ok_sem = (shown_n < real_n) if trunc else (shown_n >= real_n)
        if ok_sem:
            print(f"  ✔ 三字段齐全且语义自洽（truncated={trunc} · real={real_n} · shown={shown_n}）")
        else:
            print(f"  ✘ 语义不自洽（truncated={trunc} · real={real_n} · shown={shown_n}）")
            missing.append("语义")

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
    if missing:
        print(f"⇒ 结论：**FAIL**（覆盖量 100% ✔，但**关闭证据 ② 未满足**：{missing}）")
        return 1
    print(f"⇒ 结论：**PASS**（快照 {len(msgs)} 条 ≥ 真实 {total_real} 条 ⇒ 窗口内一条不丢；"
          f"且截断标记三字段齐全、语义自洽 ✔）")
    print(f"  另读数（**本次预期 = 覆盖量 100% + 三字段齐全自洽** ⇒ rc=0 表达的就是这件事）："
          f"｜端点耗时 {worst:.2f} s（提速验收 {'✔ 过' if latency_ok else '✘ 未过'}）"
          f"｜**⇒ 这条是独立的一格，不是 rc 的一部分**")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
