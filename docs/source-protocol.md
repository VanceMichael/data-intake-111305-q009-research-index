# 来源协议

## 记录与身份

默认来源入口为 `/feed`，媒体格式为 `json`。来源身份字段是 `cluster`、`source`、`observation`。这些字段表示来源自身声明，不等同于已经解决的业务实体身份。输入顺序是一次接收的观察顺序，并不保证业务修订有序。

`/observations/1` 等地址允许逐条读取合成观察，返回 `items` 数组。正文、页内嵌入数据与CSV均保留供方原文，不在协议层推断当前有效值。网页中的 `source-records` 是门户向下载客户端提供的公开数据块；表单还带有隐藏控件，令牌用于本地协议状态检查。

## 传输响应

`/feed` 有 ETag `feed-1`，可发送 `If-None-Match` 得到304。`/rate-limited` 返回429和两秒的Retry-After；`/unstable` 首次503，其后200；`/truncated` 声明长度大于实际传送长度并断开。`/range` 支持单段字节范围及416，`/range-ignored` 故意返回整份200，不能假定所有供方都会履行范围请求。

每次独立启动来源都有独立计数器。一个路由在 `origin.json` 中若是响应数组，依请求次序前进，最后一个响应保持不变。可通过编辑一份场景副本再用 `serve --scenario 文件` 启动不同交错序列。

## 表单回执

演练门户只接受本地POST `/portal/submit`，表单字段为 `id`、`csrf`，当前令牌由场景的 `portal.csrf` 声明。过时令牌409，缺少身份422。添加 `lose_response=yes` 会先受理再断开连接；随后GET `/portal/status?id=标识` 可看到真实回执。门户不提供提交幂等键，接收程序默认不执行提交。该端点用于受理确认和供应商核对的本地演练，不连接任何真实门户。

## 领域样例

- `requeue_attempt`：相同逻辑作业重启后等待；业务侧关注旧尝试终态不覆盖新尝试。
- `array_placeholder`：数组范围尚未物化；业务侧关注不凭占位项虚增启动或完成数。
- `late_accounting`：账务终态晚于控制器新状态；业务侧关注来源并存且未结项可继续补齐。

## 领域接入契约

采用本地规范化的 Slurm REST 只读响应子集，source区分控制器与账务。job_id不承诺跨集群或长期唯一；restart_cnt和数组、组件、steps字段各有独立层级。模型不含真实用户、脚本或凭据。REST字段依据Slurm官方接口文档，项目输入信封固定于本地场景，不依赖在线集群版本。

## 汇合口径

接收器把一条响应中的每个 `cluster`/`source`/`observation` 信封记为一次来源运行，再按下述层级解释其 `jobs`：

- 顶层尝试：`cluster` + 逻辑作业 + `submit_time` + `restart_cnt`。`array_job_id` 存在且不同于 `job_id`、或带 `array_task_id` 的记录是数组物化成员；`het_group_id` 存在且不同于 `job_id` 的记录是组件作业。两者都挂在所属逻辑作业下，不新建顶层尝试。缺少 `submit_time` 或 `restart_cnt` 的顶层记录记为 `identity_status=unresolved`，独立保存，不并入已知尝试。
- 来源侧：`controller`（或 `slurmctld`）与 `accounting`（或 `slurmdbd`）各占一行，各自保存看到的状态、是否终态与首次观察时间。两侧不按到达时间互相覆盖。
- 数组：`array_task_string` 声明的是范围，不是已启动任务。范围在成员展开前记一条 `placeholder` 成员，`materialized` 计数只统计带 `array_task_id` 的成员；物化成员的状态不影响顶层尝试的终态。
- 下级资料：`steps`、`het` 及其它组件字段记为 `step`/`component` 明细，不参与顶层计数。

“尚欠依据”指顶层尝试缺少控制器或账务任一方的终态。两侧终态相同即结项；不同则结项并记 `sides_disagree`。取消事实只由来源的 `CANCELLED`/`REVOKED` 终态产生。空响应记 `empty_observation`，`404` 等非成功响应只记接收缺口，都不改变任何尝试状态。

同侧同一尝试的观察：来源自己的非终态更新替代来源自己的旧观察；已落定的终态不会被非终态倒回；同侧另外一个终态与其不同时并列保留并记 `side_state_conflict`。每条被替代的依据仍保留在 `evidence` 中，可按 `capture_id` 取回原始正文。业务表由归档重建即可复现，不依赖实时轮询与补扫的先后次序。
