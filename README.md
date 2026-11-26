# 跨机构服务账本的分期关账与红冲

本项目的本地资料保存各方使用的原始关系、接入操作及外部观察。记录里的人员、机构和凭据均为合成。各资料目录独立使用。

## 原始关系及接入约定

account 的 kind 是机构分录类别，period 按机构和期间编号识别，opening_entries 是此前已过账分录。金额使用整数分，分录正号为借、负号为贷。先按业务凭证及币种判定总和，再按账期查询。`request` 带清算流标识、流内序号、业务请求编号及各方分摊，流内未见到的序号不能凭较晚序号已出现就视为完成。`ack_unknown` 表示调用方未收到确定回执，`refund` 的编号与原过账号不同，金额不得为负。期间可被指定 actor 关账。月度 ID 只表示账簿期间，同 ID 的两个机构分别冻结。

catalog.json 列出各数据库文件的角色、源表及运行限额。timeline.json 中的 at 是操作或观察到达的顺序，同一时刻按数组内的位置排列。args 是相应动作携带的字段，各记录里的其他位点使用该记录的业务含义。SQL 文件中只有开始联调前的行和表定义。

## 装载资料

Python 3.11.2 自带 SQLite 3.40.1。以下命令从合成资料生成独立数据库，目标文件须尚未存在。可用 sqlite3 打开生成的文件并读取源表。

~~~sh
python3 load_sample.py fixtures/overlapping-close-and-refunds main-local.db --role main
~~~

执行 python3 -m unittest -v 可核对各份资料的 SQL 是否可装载、基础表行数，以及普通 SQLite 读写连接能否并行打开。

## 可复用账本服务

本仓库现在包含仅依赖 Python 标准库的 `ledger_service` 包。它保留原始
`account`、`period`、`opening_entries`、`source_cuts` 表，并追加 `le_`
前缀的凭证、有效分录、收件箱、清算回报、退款/红冲、关账任务和稳定凭据表。

### 一分钟本地运行

```sh
# 直接装载资料并重放 timeline（目标库不能已存在）
python3 -m ledger_service replay \
  fixtures/overlapping-close-and-refunds /tmp/overlap.db \
  --output /tmp/overlap-replay.json

# 或先按仓库原方式装载，再初始化服务表
python3 load_sample.py fixtures/overlapping-close-and-refunds main-local.db --role main
python3 -m ledger_service init main-local.db

# 给财务的本地持续入口，默认 127.0.0.1:8787
python3 -m ledger_service serve main-local.db --host 127.0.0.1 --port 8787
```

也可用顶层脚本：`python3 service.py serve main-local.db`。

常用只读命令：

```sh
python3 -m ledger_service recover main-local.db
python3 -m ledger_service closed main-local.db care 03
python3 -m ledger_service net main-local.db care 03
python3 -m ledger_service service main-local.db p16
python3 -m ledger_service verify-chain main-local.db
```

### HTTP/JSON 入口

```sh
curl -s http://127.0.0.1:8787/health
curl -s http://127.0.0.1:8787/v1/recovery
curl -s 'http://127.0.0.1:8787/v1/periods/care/03/net?chain=1'
curl -s http://127.0.0.1:8787/v1/periods/care/03/certificate
curl -s http://127.0.0.1:8787/v1/services/p16
```

通用动作入口与 `timeline.json` 的 `action/args` 完全一致：

```sh
curl -s -X POST http://127.0.0.1:8787/v1/actions \
  -H 'Content-Type: application/json' \
  -d '{"action":"close_retry","args":["zhou","care","03",{"clearing-1":18}]}'
```

REST 写入入口包括：

* `POST /v1/clearing-requests`：录入清算申请到持久收件箱；
* `POST /v1/clearing-requests/{request_id}/confirm`
  或 `POST /v1/clearing-reports`：录入清算回报并过账；
* `POST /v1/refunds`、`POST /v1/refunds/{id}/retry`：跨期退款和幂等补办；
* `POST /v1/reversals`：全额或部分红字冲正；
* `POST /v1/close`、`POST /v1/periods/{org}/{month}/close`：试结账/重试冻结；
* `GET /v1/periods/{org}/{month}/net`：期间净额和凭证链；
* `GET /v1/periods/{org}/{month}/certificate`：稳定结账读凭据；
* `GET /v1/services/{ref}`：一笔业务在各机构、各期间的有效分录和更正链。

服务使用 `ThreadingHTTPServer` 并行接收请求；每个写事务执行
`BEGIN IMMEDIATE`，SQLite/WAL 加忙等待保证“余额检查 + 写入”串行化。

### 关键不变量

1. **按币种平衡**：凭证所有分录在同一事务写入；每个币种合计必须为 0，
   且账户币种必须与凭证币种一致。失败时不留有效分录。
2. **幂等与载荷指纹**：`(source, source_seq)` 和业务请求号共同识别请求。
   完全相同的重发返回原结果；金额、分摊、币种或期间变化返回
   `idempotency_conflict`，响应给出旧/新指纹，不能被旧回执遮盖。更正应使用
   新请求号并关联原凭证，或调用红冲/冲正。
3. **来源位置与缺口**：关账参数 `cuts` 给出各清算流已核对到的序号。服务会
   列出自上次 `source_cuts.final_through` 以来每个位置是 `posted`、
   `rejected`、`received_unverified`、`blocked` 还是 `missing`。未见到的
   序号不会因更晚序号已出现而视为完成。
4. **分权分期冻结**：`period` 中该机构/期间的 `actor` 才能关账；
   `(org, month)` 是独立期间，相同月份 ID 不跨机构共享冻结状态。
   `close_start` 会把期间置为 `closing`；关账 cut 内等待清算回报的申请仍可
   过账，cut 外的新申请不能挤进本次冻结。
5. **整笔凭证和共用开放期间**：若凭证任一机构期间已关/正在关且不满足 cut
   豁免，整笔过账被拒绝，不写单边分录。错误明细包含各机构期间状态和
   `next_common_open_period`；确认时传
   `"period_policy":"next_common_open"` 可整笔改到下一共用开放期间。
6. **跨期退款/红冲**：退款引用原凭证和原分摊；缺省按原借/贷分摊比例做整数
   最大余数拆分。累计已退金额和同时竞争的退款在写事务内检查，超过可退余额
   返回 `refund_limit_exceeded` 及 `available_amount`。相同退款号重试返回
   原凭证。
7. **旧账和读凭据稳定**：已过账凭证只追加，并带有序列化哈希链。关账时把
   净额、凭证哈希、来源 cut 和链尖快照写入 `le_close_certificate`，重复读取
   返回同一 `certificate_hash`/token。后续退款或更正写入新期间、新凭证并通过
   `original_voucher_id` 解释变化，不重写旧凭据。
8. **未知确认恢复**：调用方未收到 ACK 时，用相同编号/参数重试即可。
   `GET /v1/recovery` 或 `python3 -m ledger_service recover ...` 会列出哪些
   机构期间确为 `closed`、哪些申请已 `posted`、哪些仍可 confirm/retry、
   哪些退款被拒或期间阻塞。

### timeline 动作映射

`Ledger.handle_action(action, args)` 直接支持资料中的动作：

`request`、`ack_unknown`、`confirm`、`close_start`、`close_retry`、
`close_reply_unknown`、`read_closed`、`refund`、`retry`、`restart`。

同一 `at` 的记录按原数组顺序重放，符合资料说明；HTTP 入口和核心写事务另外
由并发退款测试覆盖真实并行竞争。

### 测试

```sh
python3 -m unittest -v
```

除原始装载/WAL 测试外，`test_ledger_service.py` 覆盖币种不平衡、幂等指纹
冲突、来源缺口、越权关账、关账明细、整笔期间冲突、下一共用开放期间、并发
退款上限、幂等重试、重启恢复、凭据稳定和 HTTP 并行退款。
