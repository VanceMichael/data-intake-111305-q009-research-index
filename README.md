# 科研集群 Slurm 作业资料的控制器与账务汇合采集

这是资料接收与作业资料汇合程序。它请求来源接口，保留响应状态、头部、原始内容与每次观察的时间，能够从 JSON、CSV 或门户嵌入的数据块读回来源记录。在原始归档之上，汇合层把控制器与账务的观察归并为逻辑作业视图：逻辑作业以 `(cluster, job_id, submit_time)` 标识，重启尝试按 `restart_cnt` 分层；两侧来源的证据各自保留，不按到达时间互相覆盖；数组占位范围不物化为任务，异构组件与 steps 不重复计为顶层作业；查询无记录不凭空形成事件。程序不自动执行门户提交，也不执行、取消或重新排队任何真实作业；重复观察会分别保留，原始内容按摘要复用。

## 运行环境

使用 Python 3.11 及以上，业务运行依赖均为标准库；SQLite 由 Python 自带。时区资料使用运行环境内的系统数据库。运行过程不安装依赖，也不下载数据。本地来源服务器和接收程序在同一进程环境中即可运行。

构建工具及其传递依赖精确锁定在 `requirements-build.txt`。首次准备开发环境时先执行下面命令；之后测试、构建和演示均可离线运行，不在运行命令中安装包。

```sh
python3 -m pip install -r requirements-build.txt
```

```sh
python3 -m unittest discover -s tests -v
python3 -m build --wheel --no-isolation
python3 -m compileall -q research_index tests
python3 -m research_index demo
```

演示从空的临时目录启动本地来源，真实完成一次 HTTP 获取及归档，输出原样记录与汇合视图摘要；结束自动清理。本地测试、编译和演示合计一般不超过十秒，可直接展示，不依赖历史日志。

## 作业资料汇合

```sh
python3 -m research_index capture --base-url http://127.0.0.1:实际端口 --archive .runtime/raw.sqlite3 --source local-source --path /feed --merge
python3 -m research_index reconcile --archive .runtime/raw.sqlite3
python3 -m research_index jobs --archive .runtime/raw.sqlite3
python3 -m research_index job --archive .runtime/raw.sqlite3 --cluster science-a --job-id 71
python3 -m research_index outstanding --archive .runtime/raw.sqlite3
python3 -m research_index raw --archive .runtime/raw.sqlite3 --capture-id 1
```

`capture --merge` 在捕获后立即汇入台账；`reconcile` 把尚未汇入的捕获补入，可反复执行，进程重启后继续运行即可补齐未结项。`outstanding` 列出尚欠账务终态的尝试。`job` 按逻辑作业展示各尝试、数组成员与异构组件，证据条目给出 `capture_id`，用 `raw` 下钻到源响应。模型细则见 `docs/job-merge.md`。

## 常驻来源与接收

```sh
python3 -m research_index serve
```

终端会输出本次分配的回环地址。在另一终端将下面地址替换为实际地址：

```sh
python3 -m research_index capture --base-url http://127.0.0.1:实际端口 --archive .runtime/raw.sqlite3 --source local-source --path /feed
```

归档库只追加来源观察，不用最后一条响应覆盖先前响应。`archive.py` 管理原始内容及完整性，`client.py` 执行一次请求，`decoding.py` 负责来源格式解码，`origin.py` 提供协议演练，`jobs.py` 在归档之上汇合作业资料。业务解释可与原始接收独立演进。

## 开发资料

来源协议见 `docs/source-protocol.md`，汇合模型见 `docs/job-merge.md`，领域观察关系见 `tests/test_source_contract.py`。`fixtures/business_cases.json` 保存业务侧关注的交错情况及可观察口径，汇合层测试（`tests/test_jobs.py`）逐一覆盖这些情况。所有条目、机构、文件内容和门户回执均为本地合成资料，不包含真实用户身份。

## Wheel 分发

构建产物位于 `dist/`，包含 `research_index` 和命名空间内的合成来源 JSON。安装 wheel 后，即使当前目录不是源码目录，也可执行 `python3 -m research_index demo`。提供 `serve --scenario` 时仍以指定文件为准，找不到指定文件会直接报错，不回退到默认资料。
