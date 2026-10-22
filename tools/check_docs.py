#!/usr/bin/env python3
"""check_docs.py —— 文档有效性自检（标准库 · 零依赖）

三件事：
  1. 相对链接可解析：README / 指南里每个 [文字](相对路径) 都要指向真实存在的文件或锚点
  2. 语言镜像齐：中文文档（无后缀）与英文文档（.en.md）必须成对存在
  3. 跨项目残留：文档里不许出现别的项目的关键字（电抗器 / 电磁 / 零碳 / ESP32 / 银河 …）

用法：
    python3 tools/check_docs.py            # 打印报告，非零退出码 = 有问题
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS = ["README.md", "README.zh.md", "README.en.md", "docs/部署对接指南.md", "docs/deployment-guide.en.md"]
LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)")
FOREIGN = ["电抗器", "电磁", "零碳", "ESP32", "银河战舰", "galaxy-wars", "reactor"]

problems = []


def doc_paths():
    out = []
    for name in DOCS:
        p = os.path.join(ROOT, name)
        if os.path.exists(p):
            out.append(p)
    return out


def check_links():
    for path in doc_paths():
        base = os.path.dirname(path)
        with open(path, encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                for target in LINK.findall(line):
                    if target.startswith(("http://", "https://", "mailto:", "#")):
                        continue
                    rel = target.split("#")[0]
                    if not rel:
                        continue
                    if not os.path.exists(os.path.join(base, rel)):
                        problems.append("坏链  %s:%d  ->  %s" % (os.path.relpath(path, ROOT), i, target))


def check_mirrors():
    pairs = [("README.md", "README.en.md"), ("docs/部署对接指南.md", "docs/deployment-guide.en.md")]
    for zh, en in pairs:
        zhp, enp = os.path.join(ROOT, zh), os.path.join(ROOT, en)
        if os.path.exists(zhp) and not os.path.exists(enp):
            problems.append("语言缺一半  有 %s 缺 %s" % (zh, en))
        if os.path.exists(enp) and not os.path.exists(zhp):
            problems.append("语言缺一半  有 %s 缺 %s" % (en, zh))


def check_foreign():
    for path in doc_paths():
        with open(path, encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                for kw in FOREIGN:
                    if kw in line:
                        problems.append("跨项目残留  %s:%d  含「%s」" % (os.path.relpath(path, ROOT), i, kw))


check_links()
check_mirrors()
check_foreign()

if problems:
    print("文档自检：%d 项待修" % len(problems))
    for p in problems:
        print(" -", p)
    sys.exit(1)
print("文档自检：全部通过（链接可解析 · 语言成对 · 无跨项目残留）")
