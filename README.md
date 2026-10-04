# raft-lease-coordinator

三节点租约锁后端：基于 HashiCorp Raft 1.7.3（Go 1.27.1）的多进程独占资源协调服务。
每个节点是独立进程，拥有独立数据目录，通过真实 TCP 复制日志，HTTP 对外提供
锁的申请、续租、释放与查询。

## 结构

| 文件 | 职责 |
| --- | --- |
| `internal/fsm/fsm.go` | 租约状态机：申请/续租/释放判定、防护令牌上界、快照与恢复 |
| `internal/node/node.go` | Raft 节点：TCP 传输、BoltDB 日志/任期/投票持久化、首次引导 |
| `internal/api/http.go` | HTTP API：leader 校验、请求校验、提交超时处理 |
| `cmd/lockd/main.go` | 进程入口：解析配置、启动节点、信号退出时关闭网络与存储 |
| `internal/fsm/fsm_test.go` | 状态机自测（争用、令牌单调、过期、快照恢复） |

屏障（barrier）是租约之上的可恢复阶段会合点：先独立申请资源租约，再把本次阶段
登记复制到 Raft；只有全部固定成员到达且登记租约在同一提交时刻仍匹配，本轮才完成。

## 构建与测试

```sh
go build ./...
go test ./internal/...
go build -o bin/lockd ./cmd/lockd
```

## 配置

每个节点需要：节点 ID、Raft 地址、HTTP 地址、数据目录，以及固定的三名投票成员
（含自身，格式 `id=raftAddr=httpAddr`，逗号分隔）。仅在数据目录无任何既有状态时
才引导集群；已有日志/快照的节点不会重新引导。不提供动态成员管理。

## 启动三节点（同机）

```sh
PEERS="n1=127.0.0.1:7101=127.0.0.1:8101,n2=127.0.0.1:7102=127.0.0.1:8102,n3=127.0.0.1:7103=127.0.0.1:8103"
./bin/lockd -id n1 -raft-addr 127.0.0.1:7101 -http-addr 127.0.0.1:8101 -data-dir data/n1 -peers "$PEERS" &
./bin/lockd -id n2 -raft-addr 127.0.0.1:7102 -http-addr 127.0.0.1:8102 -data-dir data/n2 -peers "$PEERS" &
./bin/lockd -id n3 -raft-addr 127.0.0.1:7103 -http-addr 127.0.0.1:8103 -data-dir data/n3 -peers "$PEERS" &
```

## HTTP API

只有 leader 处理操作；follower 返回 `307` 及已知 leader 的 HTTP 地址，绝不在本地成功。
多数派提交并应用后才响应成功；提交超时返回 `503`，结果未确认。

- `POST /acquire` `{"resource":"r","holder":"h","ttl":30}`
  - `ttl` 取 1-300 秒。无有效租约才授予；成功返回 `holder`、`expiry`（Unix 秒）
    和逐资源递增的防护令牌 `token`。
  - 已被占用返回 `409` 及当前持有人信息。
- `POST /renew` `{"resource":"r","holder":"h","token":1,"ttl":30}`
  - 必须匹配持有人与令牌；过期请求拒绝且不影响后来持有人。
  - 截止时间从判定时刻（leader 写入命令的时间）重新计算。
- `POST /release` `{"resource":"r","holder":"h","token":1}`
  - 必须匹配持有人与令牌；释放不清除该资源的令牌上界。
- `GET /query?resource=r`
  - 查询同样由 leader 作为带判定时间的命令提交，按已提交日志返回；不直接读本地 FSM。
  - 返回当前有效租约；已到期返回 `{"ok":false}`。

### 屏障

- `POST /barriers` `{"name":"phase","participants":["p1","p2"]}`
  - 创建命名屏障；参与者数量必须为 2 至 16，ID 不可重复，成员创建后固定。
  - 初始轮号为 1；同名同成员配置幂等，同名异配置返回冲突。
- `POST /barriers/arrive`
  - 请求字段：`name`、`round`、`participant`、`resource`、`holder`、`token`。
  - 参与者必须先用旧接口取得自己的独立资源租约；到达命令只接纳持有人和防护令牌都匹配且在判定时刻未到期的租约。
  - 同一轮资源不可被重复登记；同一参与者重复提交完全相同登记幂等，不重复计数；未知参与者、旧轮号、同参与者换登记均拒绝。
  - 最后一人到达时，在同一个已提交命令中再次校验全部登记租约，全部有效才置为 `completed`；任一已释放、到期或换令牌则本轮置为 `failed`，不能替补。
  - 等待期间租约失效不依赖后台定时器，最迟在下一次创建、到达、推进或查询该屏障的复制命令判定；完成态和失败态不会倒退。
- `GET /barriers?name=phase`
  - 返回轮号、状态（`waiting`/`completed`/`failed`）、固定成员、已到达 ID 与失败原因。
- `POST /barriers/advance` `{"name":"phase","round":1}`
  - `round` 是预期当前轮号；只有 `completed` 或 `failed` 终态可以推进。
  - 推进后轮号加一、状态回到 `waiting`、清空上一轮到达记录；并发推进中只有匹配当前轮号的一次生效，其余因预期轮号过期而拒绝。

## 一致性设计

- 判定时刻由 leader 写入日志命令（`Command.Now`），副本重放不读取本地时钟；
  所有操作按已提交日志顺序在状态机中判定，并发争用只授予一方。
- 租约查询和屏障查询也是 Raft 命令：leader 把查询时刻写入命令，多数派提交后才由状态机返回，避免 follower 旧状态或 leader 本地读造成脏读。
- 防护令牌按资源单调递增，上界保存在快照中，释放/到期/重启/快照恢复均不回退。
- 日志、任期、投票持久化在 BoltDB（`data/<id>/raft.db`）；快照包含当前租约、每资源令牌上界，以及全部屏障配置、轮号、状态和登记，重启或换主后不接纳旧轮请求。
- 不含屏障字段的旧快照仍可恢复；缺失屏障集合按空状态初始化。
- leader 失效后剩余两节点可选举并继续服务；不足多数派时不授予任何锁。

## 演示（curl）

```sh
# 争用：alice 成功，bob 冲突
curl -X POST 127.0.0.1:8101/acquire -d '{"resource":"resA","holder":"alice","ttl":30}'
curl -X POST 127.0.0.1:8101/acquire -d '{"resource":"resA","holder":"bob","ttl":30}'
# 续租（需匹配 token）
curl -X POST 127.0.0.1:8101/renew -d '{"resource":"resA","holder":"alice","token":1,"ttl":60}'
# 查询 / 释放
curl "127.0.0.1:8101/query?resource=resA"
curl -X POST 127.0.0.1:8101/release -d '{"resource":"resA","holder":"alice","token":1}'
# 释放后重取获得更大令牌
curl -X POST 127.0.0.1:8101/acquire -d '{"resource":"resA","holder":"bob","ttl":30}'
```

## 屏障演示（curl）

```sh
# 创建两成员屏障（参与者固定，轮号从 1 开始）
curl -X POST 127.0.0.1:8101/barriers \
  -d '{"name":"phase","participants":["worker-1","worker-2"]}'

# 两个进程分别使用旧接口获取不同资源的独立租约
curl -X POST 127.0.0.1:8101/acquire -d '{"resource":"job-1","holder":"worker-1","ttl":60}'
curl -X POST 127.0.0.1:8101/acquire -d '{"resource":"job-2","holder":"worker-2","ttl":60}'

# 依次报告到达；第二次返回 completed，相同请求重发幂等
curl -X POST 127.0.0.1:8101/barriers/arrive \
  -d '{"name":"phase","round":1,"participant":"worker-1","resource":"job-1","holder":"worker-1","token":1}'
curl -X POST 127.0.0.1:8101/barriers/arrive \
  -d '{"name":"phase","round":1,"participant":"worker-2","resource":"job-2","holder":"worker-2","token":1}'
curl '127.0.0.1:8101/barriers?name=phase'

# 终态后才允许开启第 2 轮，并清空到达记录
curl -X POST 127.0.0.1:8101/barriers/advance -d '{"name":"phase","round":1}'

# 失败演示：只到达一方后释放租约，下一次查询在已提交日志中判定 failed；不能补登记
curl -X POST 127.0.0.1:8101/barriers \
  -d '{"name":"fail-demo","participants":["worker-1","worker-2"]}'
curl -X POST 127.0.0.1:8101/acquire -d '{"resource":"fail-job","holder":"worker-1","ttl":60}'
curl -X POST 127.0.0.1:8101/barriers/arrive \
  -d '{"name":"fail-demo","round":1,"participant":"worker-1","resource":"fail-job","holder":"worker-1","token":3}'
curl -X POST 127.0.0.1:8101/release -d '{"resource":"fail-job","holder":"worker-1","token":3}'
curl '127.0.0.1:8101/barriers?name=fail-demo'
curl -X POST 127.0.0.1:8101/barriers/advance -d '{"name":"fail-demo","round":1}'
```
