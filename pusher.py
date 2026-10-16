#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cumora 实时推送器（方案 A：主动提交，替代 server 轮询全量解析）
- 常驻跟随 ~/.cumora/daemon.log 追加（事件驱动，非轮询扫描）
- 攒批 POST 到 server /api/ingest 增量端点；server 指纹去重后喂入状态机
- 轮转处理：inode 变化 → 先通知 server 重建（rotate）→ 确认后从新文件当前末尾续推
- 游标持久化（pusher.cursor.json），重启不重推历史
用法: python3 pusher.py [port]   (默认 8787)
"""
import json, os, time, urllib.request, sys

LOG   = os.path.expanduser("~/.cumora/daemon.log")
BASE  = os.path.dirname(os.path.abspath(__file__))
CURSOR = os.path.join(BASE, "pusher.cursor.json")
PORT  = int(sys.argv[1]) if len(sys.argv) > 1 else 8787
API   = "http://127.0.0.1:%d/api/ingest" % PORT
POLL  = 0.5        # 文件轮询间隔（事件驱动语义，非周期全量扫描）
BATCH = 300        # 单批推送行数

def post(payload, retries=4):
    data = json.dumps(payload).encode("utf-8")
    for i in range(retries):
        try:
            req = urllib.request.Request(API, data=data,
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=6) as r:
                return json.loads(r.read().decode("utf-8", errors="ignore"))
        except Exception:
            time.sleep(1.0 + i)
    return None

def save_cursor(cursor):
    try:
        with open(CURSOR, "w") as f:
            json.dump(cursor, f)
    except Exception:
        pass

def load_cursor():
    try:
        with open(CURSOR) as f:
            return json.load(f)
    except Exception:
        return None

def main():
    cursor = load_cursor()
    cur_ino = cursor.get("ino") if cursor else None
    cur_off = cursor.get("off", 0) if cursor else 0
    print("[pusher] 启动 · 游标 ino=%s off=%d · 目标 %s" % (cur_ino, cur_off, API))
    # 无游标：首次从当前文件末尾开始（历史由 server 启动 full_scan 覆盖，避免双计）
    if cur_ino is None:
        try:
            stt = os.stat(LOG)
            cur_ino, cur_off = stt.st_ino, stt.st_size
            save_cursor({"ino": cur_ino, "off": cur_off})
            print("[pusher] 首次启动 · 从当前末尾续推 (ino=%s off=%d)" % (cur_ino, cur_off))
        except OSError:
            print("[pusher] 警告: %s 不存在，等待..." % LOG)

    pending = []
    last_flush = time.time()
    while True:
        flushed = False
        try:
            stt = os.stat(LOG)
        except OSError:
            time.sleep(1.0)
            continue
        # 轮转检测
        if cur_ino is not None and stt.st_ino != cur_ino:
            print("[pusher] 检测到轮转 %s -> %s，通知 server 重建..." % (cur_ino, stt.st_ino))
            post({"rotate": True, "ino": stt.st_ino})   # server 重建后返回
            cur_ino = stt.st_ino
            cur_off = stt.st_size                      # 重建已覆盖历史，从当前末尾续推
            save_cursor({"ino": cur_ino, "off": cur_off})
            print("[pusher] 轮转重建完成 · 续推 off=%d" % cur_off)
        elif cur_ino is None:
            cur_ino = stt.st_ino
            cur_off = stt.st_size
        size = stt.st_size
        if size < cur_off:      # 文件被截断
            cur_off = 0
        if size > cur_off:
            with open(LOG, "rb") as f:
                f.seek(cur_off)
                chunk = f.read(min(size - cur_off, 2 << 20))
            cur_off = size
            lines = chunk.decode("utf-8", errors="ignore").splitlines()
            if lines:
                pending.extend(lines)
                save_cursor({"ino": cur_ino, "off": cur_off})
        # 攒批推送
        if len(pending) >= BATCH or (pending and time.time() - last_flush >= 1.0):
            batch, pending = pending, []
            r = post({"lines": batch})
            if r is None:
                pending = batch + pending      # 失败回退重推（server 指纹去重，不会双计）
                print("[pusher] ingest 失败，%d 行待重推" % len(pending))
            else:
                print("[pusher] 推 %d 行 → accepted=%s skipped=%s" % (len(batch), r.get("accepted"), r.get("skipped")))
            last_flush = time.time()
            flushed = True
        time.sleep(POLL)

if __name__ == "__main__":
    main()
