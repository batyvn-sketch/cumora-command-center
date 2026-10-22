# Cumora 指挥中心（本地实时可视化）

一个**只读**的本地服务：把 Cumora 的 daemon 日志解析成一份快照，用一个单页 HTML 画出来 —— 今日消息、任务、产出物、agent 活跃、沟通线、看板、日程、token 用量。

- **服务**：`server.py`（Python 3 标准库，无第三方依赖）
- **页面**：同目录 `Cumora指挥中心.html`（单文件，无构建步骤）
- **数据源**：`~/.cumora/daemon.log`（只读；服务不写任何东西）
- **默认端口**：`8787`

## 30 秒跑起来

```bash
cd /Users/a1234/pi/cumora-command-center
python3 server.py 8787
# 浏览器打开 http://127.0.0.1:8787/
```

首启 macOS 会拦一次（Gatekeeper）→ 点「仍要打开」。页面如果看着像旧版 ⇒ **硬刷**（Cmd+Shift+R），缓存会把旧版留住。

## 环境变量

| 变量 | 作用 | 默认 |
|---|---|---|
| `CC_PORT` | 监听端口 | `8787`（也可用 `python3 server.py <port>`） |
| `CC_LOG` | daemon 日志路径 | `~/.cumora/daemon.log` |
| `CC_HTML` | 指定页面文件 | 同目录 `Cumora指挥中心.html`（也认 `index.html`） |

## 怎么确认它真的在跑（三个自检点）

1. **今日消息条数会随群里说话变** —— 在任意群里发一句，刷新后条数应 +1
2. **沟通线的两端是真 agent** —— 不是占位圆点
3. **agent 肖像 7 张** —— 对应 nina / nova / iris / atlas / sage / bram / peta

## 接口

| 路径 | 说明 |
|---|---|
| `GET /` | 单页页面（HTML） |
| `GET /api/snapshot` | JSON 快照：最近 4 小时窗口 + token 全量累计（CORS 开放） |

`/api/snapshot` 主要字段：`tasks`（任务，含 `agent` / `artifacts` / `steps` / `status` / `start` / `end`）· `artifacts`（产出物：`t` 时间 / `a` agent / `g` 群 / `path`，**即将补 `task` 任务指针**）· `agents`（id / name / role / status）· `groups` · `messages` · `events` · `comm`（沟通线）· `tokens_g` / `tokens_all`。

## 目录结构

```
server.py                    # 本地数据服务（唯一入口）
Cumora指挥中心.html          # 单页可视化
pusher.py                    # 推送到远端的小工具
assets/                      # 页面静态资源
_backup/                     # 历史版本备份（内部迭代留痕）
docs/部署对接指南.md          # 部署 / 对接 / 排障 / 变更纪律
```

## 已知限制

- **没有进程守护** —— 服务挂了没人知道（建议自己挂个 launchd / 定时自启）
- **端口占用会直接起不来** —— 先 `lsof -nP -iTCP:8787 -sTCP:LISTEN` 看看
- **无 OpenGL 的机器上 3D 帧会被跳过** —— 图 9/10 的 3D 部分没有，其余正常
- **页面不缓存数据** —— 每次刷新都现读 daemon.log

## 变更纪律（两条，踩过坑）

- **落点可追 = 有 commit + 作者是谁** —— 共享的 `.git/config` 里 `user.name` 是谁，所有人的提交就都记成谁 ⇒ **每人第一次提交前先 `git config user.name "你的名字"` + `git config user.email "你的邮箱"`**（一次配置，永久正确）
- **唯一源只有一处** —— 别把同一份文件放两个地方（放了两处就有一处会过期）

## 远端

`https://github.com/batyvn-sketch/cumora-command-center`（public · main）
