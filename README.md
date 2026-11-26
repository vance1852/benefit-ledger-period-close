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
