# 作业资料汇合

`research_index/jobs.py` 在原始归档（captures）之上维护逻辑作业视图。汇合表与原始捕获同库存放、只引用不修改；删除汇合表后执行 `reconcile` 可从归档完整重建。汇合层只读来源接口，不执行、取消或重新排队任何真实作业。

## 身份层级

- **逻辑作业**以 `(cluster, job_id, submit_time)` 标识。`job_id` 不承诺跨集群或长期唯一，不能单独充当全局键；`submit_time` 缺失时按未知值独立成键，不与已知值猜测合并。
- **尝试**在逻辑作业内按 `restart_cnt` 分层。重排队产生新尝试，旧尝试的终态不覆盖新尝试。来源未报告 `restart_cnt` 时视为 0。
- **数组成员**：记录带 `array_task_id` 才是已物化成员，单独计为一个任务并链接到 `array_job_id`。
- **数组占位**：只有 `array_task_string`（如 `1-4`）的记录是未物化范围，不物化出任务、不计入启动或完成数；范围作为属性保留在逻辑作业上。
- **异构组件**：`het_job_offset > 0` 的记录是组件，归属 `het_job_id` 对应的领头作业，不单独计为顶层作业；offset 为 0 的是领头作业。
- **steps** 归属单次尝试的账务证据，永远不是作业，不参与任何作业计数。

## 来源并存

控制器与账务的证据分行保存（`evidence` 表按 尝试 + 来源 + 内容摘要 去重；同一内容经不同捕获重复到达只留一条，原始捕获仍各自保留）。两侧暂时不一致时各自保留依据，不按到达时间互相覆盖：

- 每个尝试的"当前视图"按来源分别给出；同一来源内按来源自报的 `observation` 序号识别新观察，缺序号时按到达顺序。
- 全部证据行都在 `job` 详情中列出，可经 `capture_id` 用 `raw` 命令下钻到源响应。

## 未结项与补扫

尝试缺少账务终态（见 `TERMINAL_STATES`）即为未结项，`outstanding` 持续列出并标注是否运行过（`ran`）。未结项持久保存，进程重启后仍在；补扫（`reconcile`）或实时轮询（`capture --merge`）汇入迟到的账务观察后自动结清。接口保留期不同导致某次查询没有记录时，不形成任何事件，更不会凭空产生取消。

## 命令

```sh
python3 -m research_index capture --base-url 地址 --archive 库文件 --source 名称 --path /feed --merge
python3 -m research_index reconcile --archive 库文件
python3 -m research_index jobs --archive 库文件 [--cluster 集群]
python3 -m research_index job --archive 库文件 --cluster 集群 --job-id 编号 [--submit-time 时间]
python3 -m research_index outstanding --archive 库文件
python3 -m research_index raw --archive 库文件 --capture-id 编号
```

`reconcile` 把尚未汇入的捕获补入台账，可反复执行；无法解码的捕获按条报错、不阻塞其余，修正后重扫即可。`job_id` 复用时 `job` 会要求用 `--submit-time` 区分。
