# Deployment & Integration Guide · Cumora Command Center

For two kinds of reader: **you want to run the service** · **you want to wire it into your own flow**.

> **Language**: **[English](deployment-guide.en.md)** · **[简体中文](部署对接指南.md)** (the page UI ships `en`/`zh`/`ja`/`ko`/`fr`; the docs are mirrored per language ⇒ see "Change discipline" rule 3 in section 6)

---

## 1. Prerequisites

| Item | Requirement |
|---|---|
| Python | 3.9+ (standard library only · **no third-party dependencies**) · 3.14 dev builds fine too |
| Port | 8787 by default (changeable) |
| Data source | `~/.cumora/daemon.log` (the Cumora daemon log · **read-only**) |
| Disk | repo ≈ 8 MB (`_backup/` is iteration history, safe to delete) |

## 2. Deployment (three scenarios)

### A. Run it locally (most common)

```bash
cd /Users/a1234/pi/cumora-command-center
python3 server.py 8787
```

- Opening the packaged `.app` is blocked once by macOS (Gatekeeper) → click "Open Anyway" (**running the script `python3 server.py` is not blocked** — the repo's files carry no quarantine attribute)
- Run it in the background: `nohup python3 server.py 8787 > /tmp/cc.log 2>&1 &`
- Verify: `curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8787/api/snapshot` ⇒ expect `200`

### B. On another machine

```bash
git clone https://github.com/batyvn-sketch/cumora-command-center.git
cd cumora-command-center
python3 server.py 8787
```

**Note**: the data source is **this machine's** `~/.cumora/daemon.log` ⇒ after moving, either bring the daemon log over (`CUMORA_LOG=/path/to/daemon.log`) or run Cumora on that machine ⇒ **the service never connects to a remote** (it only reads a local file).

### C. Keep it alive (so it does not die at night)

launchd (macOS) minimal example → `~/Library/LaunchAgents/com.cumora.cc.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>com.cumora.cc</string>
  <key>ProgramArguments</key>
  <array><string>/usr/bin/python3</string><string>/Users/a1234/pi/cumora-command-center/server.py</string><string>8787</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
</dict></plist>
```

```bash
launchctl load ~/Library/LaunchAgents/com.cumora.cc.plist
```

## 3. Integration (feeding the data into your own flow)

**The only interface**: `GET http://127.0.0.1:8787/api/snapshot` (JSON · CORS open · last 4 hours window + all-time token totals)

| Field | Content | Note |
|---|---|---|
| `tasks` | task: `id` / `agent` / `group` / `status`(running/done) / `start` / `end` / `dur_s` / `steps` / `artifacts` / `tools` / `msgs` / `title` | `tasks` is the **done** history (cap 120) |
| `running_tasks` | number of running tasks | |
| `artifacts` | artifact: `t`(time) / `a`(agent) / `g`(group) / `path` | **a `task` field is being added** (artifact ↔ task pointer); with no real pointer it is inferred as "same agent + artifact time inside that task's [start_ts, end_ts]" and tagged `task_match: "inferred"` |
| `agents` | 7 of them: `id` / `name` / `role` / `status` | |
| `groups` | 4 groups | `GID2SHORT` map lives in `server.py` |
| `messages` | message stream (cap 400) | |
| `events` | event stream (cap 1200) | |
| `comm` | comms lines (both ends = two agents) | |
| `tokens_g` / `tokens_all` | token usage: per group / per agent | |
| `generated_at` / `window_h` | snapshot timestamp / window size in hours | |

**Suggested wiring**:
- **Read-only** ⇒ GET the snapshot, do not parse daemon.log yourself (that is the service's job)
- **Polling** ⇒ once every 1–2 s is enough (that is what the page does)
- **Do not cache the snapshot** ⇒ if you need it live, read it live (the page re-reads on every refresh)

## 4. Reading the "task — artifact" lines on the page (new feature)

- A line = **artifact ↔ task** (the task then links to the agent)
- **Each agent keeps the latest 3** (from the 4th on, it is not drawn)
- Older lines are **30% opaque**, the newest one 100%
- When a new artifact appears ⇒ that agent's old lines drop to 30% immediately, and anything beyond 3 is removed
- **Acceptance**: for every agent, line count ≤ 3, and each line connects an artifact node to a task node

## 5. Troubleshooting

| Symptom | Cause | What to do |
|---|---|---|
| Blank page / error | port not up | `lsof -nP -iTCP:8787 -sTCP:LISTEN` to check for a process; start it if none |
| Page looks stale | browser cache | **hard-refresh** (Cmd+Shift+R) |
| Page empty but the API has data | wrong page file | point `CC_HTML` at the right file; default is `Cumora指挥中心.html` next to server.py |
| No new message in the stream | wrong log path | point `CUMORA_LOG` at the real `~/.cumora/daemon.log`; `ls -l` it first to see whether it exists and grows (**note**: the top counter is the 4-hour-window count and need not move even when the message appears) |
| 3D frames missing (figures 9/10) | machine without OpenGL | normal (everything else works) |
| Port in use | an old instance is still running | `kill <pid>` or use `CC_PORT` |
| Service died at night | no supervisor | see "C. Keep it alive" above |

## 6. Change discipline (three rules, each from a real bug)

1. **A traceable landing point = a commit + who authored it**
   the shared `.git/config` has one `user.name`, so everyone's commit records as whoever that is ⇒ **before your first commit, set `git config user.name "Your Name"` + `git config user.email "you@example.com"`** ⇒ existing history is never rewritten (rewriting moves other people's commits too)
2. **Exactly one source of truth**
   do not keep the same file in two places — two copies ⇒ one of them expires, and people only read the expired one
3. **Docs in many languages = all at once**
   mirror every doc across the five languages the page supports (`en` / `zh` / `ja` / `ko` / `fr`); **adding one language means adding all five** (this guide + the root README) · section by section · change a paragraph ⇒ change it in five places · **currently `en` + `zh` are in** (the page UI already ships all five)

## 7. Layout

```
server.py                    # single entry point (local data service)
Cumora指挥中心.html          # single-page visualization
pusher.py / pusher.cursor.json
assets/                      # static page resources
_backup/                     # historical backups (internal iteration trail)
docs/部署对接指南.en.md      # this file
docs/部署对接指南.md          # 部署对接指南（简体中文）
README.en.md / README.md     # intro / how to run (English / Simplified Chinese)
assets/commcenter-shot.png   # UI screenshot used by the README
```
