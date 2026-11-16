"""控制器与账务来源的作业资料汇合。

这一层只解释已经归档的原始观察，不发起真实作业操作，也不改变任何真实作业的
状态。它解决四类混淆：

* ``job_id`` 不是全局唯一键。逻辑作业由 ``cluster``、``job_id`` 与
  ``submit_time`` 共同确定；同一 ``job_id`` 在不同集群、不同提交下互不相干。
* 重启产生新的尝试。``restart_cnt`` 把同一逻辑作业的多次运行分开，因此一次
  重排队不是两个作业。
* 数组占位范围（``array_task_string`` 中的区间）没有展开时不是已启动任务，
  不参与启动或完成计数；只有物化后的成员才计数。
* 组件（``het``/``het_group_id``）与 ``steps`` 属于下级层级，作为组件的资料
  保留，不重复计为顶层作业。

控制器通常先看到运行状态，账务可能延迟才补齐终态与步骤。两侧各自保存自己的
观察；一侧的资料不会按到达时间覆盖另一侧。缺少一侧终态的运行被标记为“尚欠
依据”，可继续补扫。某次查询没有记录只说明没有记录，绝不推导取消事件。

所有汇合状态都写入调用方给定的归档库（独立于 ``captures`` 原始表），进程重启
后可直接从库中继续补齐未结项。``reconcile`` 只重算本地状态，幂等。
"""
import json
import re

from .decoding import records as decode_records

SCHEMA = """
CREATE TABLE IF NOT EXISTS source_runs (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    capture_id INTEGER NOT NULL REFERENCES captures(id),
    item_index INTEGER NOT NULL,
    cluster TEXT,
    declared_source TEXT,
    declared_observation TEXT,
    received_at TEXT NOT NULL,
    record_count INTEGER NOT NULL DEFAULT 0,
    processing TEXT NOT NULL,
    UNIQUE (capture_id, item_index)
);
CREATE TABLE IF NOT EXISTS run_records (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES source_runs(id),
    job_id TEXT,
    logical_job_id TEXT,
    submit_time TEXT,
    restart_cnt INTEGER,
    role TEXT NOT NULL,
    array_task_id TEXT,
    het_group_id TEXT,
    identity_status TEXT NOT NULL,
    detail TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS attempts (
    id INTEGER PRIMARY KEY,
    attempt_key TEXT NOT NULL UNIQUE,
    cluster TEXT NOT NULL,
    job_id TEXT NOT NULL,
    submit_time TEXT,
    restart_cnt INTEGER,
    identity_status TEXT NOT NULL,
    top_level INTEGER NOT NULL DEFAULT 1,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS attempt_sides (
    attempt_id INTEGER NOT NULL REFERENCES attempts(id),
    side TEXT NOT NULL,
    run_id INTEGER NOT NULL REFERENCES source_runs(id),
    record_id INTEGER REFERENCES run_records(id),
    state TEXT,
    final INTEGER NOT NULL,
    cancelled INTEGER NOT NULL,
    observed_at TEXT NOT NULL,
    PRIMARY KEY (attempt_id, side)
);
CREATE TABLE IF NOT EXISTS attempt_details (
    id INTEGER PRIMARY KEY,
    attempt_id INTEGER NOT NULL REFERENCES attempts(id),
    side TEXT NOT NULL,
    run_id INTEGER NOT NULL REFERENCES source_runs(id),
    record_id INTEGER REFERENCES run_records(id),
    kind TEXT NOT NULL,
    label TEXT NOT NULL,
    detail TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    UNIQUE (attempt_id, side, kind, label)
);
CREATE TABLE IF NOT EXISTS attempt_members (
    attempt_id INTEGER NOT NULL REFERENCES attempts(id),
    source_task TEXT NOT NULL,
    task_index INTEGER,
    materialized INTEGER NOT NULL,
    observations INTEGER NOT NULL DEFAULT 0,
    last_state TEXT,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    PRIMARY KEY (attempt_id, source_task)
);
CREATE TABLE IF NOT EXISTS attempt_arrays (
    attempt_id INTEGER PRIMARY KEY REFERENCES attempts(id),
    declared_range TEXT,
    placeholder INTEGER NOT NULL,
    first_seen TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS evidence (
    id INTEGER PRIMARY KEY,
    attempt_id INTEGER REFERENCES attempts(id),
    attempt_key TEXT NOT NULL,
    side TEXT,
    run_id INTEGER NOT NULL REFERENCES source_runs(id),
    record_id INTEGER REFERENCES run_records(id),
    kind TEXT NOT NULL,
    label TEXT,
    payload TEXT NOT NULL,
    observed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    attempt_key TEXT,
    action TEXT NOT NULL,
    detail TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS attempt_status (
    attempt_id INTEGER PRIMARY KEY REFERENCES attempts(id),
    settled INTEGER NOT NULL,
    outstanding TEXT NOT NULL,
    conflict INTEGER NOT NULL,
    reconciled_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ingest_failures (
    capture_id INTEGER PRIMARY KEY REFERENCES captures(id),
    reason TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_attempt_action ON events(attempt_key, action);
CREATE INDEX IF NOT EXISTS run_records_run ON run_records(run_id);
"""

# Slurm 的终态集合；未知状态既不当作运行中，也不当作完成。
TERMINAL_STATES = frozenset((
    "BOOT_FAIL", "CANCELLED", "COMPLETED", "DEADLINE", "FAILED",
    "NODE_FAIL", "OUT_OF_MEMORY", "PREEMPTED", "REVOKED", "TIMEOUT",
))
CANCELLED_STATES = frozenset(("CANCELLED", "REVOKED"))
# 只有这两个来源参与“尚欠依据”的计算；其它来源仍保留其观察。
SIDES = ("controller", "accounting")
_SIDE_ALIASES = {
    "controller": "controller",
    "slurmctld": "controller",
    "ctld": "controller",
    "accounting": "accounting",
    "slurmdbd": "accounting",
    "dbd": "accounting",
}
_STATE_SPLIT = re.compile(r"[\s,]+")
_TASK_TOKEN = re.compile(r"^(\d+)(?:-(\d+))?(?::(\d+))?$")


def normalize_side(value):
    text = _text(value)
    if text is None:
        return None
    key = text.lower()
    return _SIDE_ALIASES.get(key, "other:" + key)


def _text(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


def _state_of(job):
    """返回 (state, final)。来源显式 final 标记优先于终态集合。"""
    raw = job.get("state")
    if isinstance(raw, (list, tuple)):
        tokens = [str(item).strip() for item in raw if _text(item) is not None]
    elif raw is None:
        tokens = []
    else:
        tokens = [token for token in _STATE_SPLIT.split(str(raw).strip()) if token]
    state = tokens[0].upper() if tokens else None
    if isinstance(job.get("final"), bool):
        final = job["final"]
    else:
        final = state in TERMINAL_STATES
    return state, final


def _list_of_objects(value):
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _steps(job):
    for name in ("steps", "job_steps"):
        value = job.get(name)
        if isinstance(value, list):
            return _list_of_objects(value)
    return []


def _components(job):
    for name in ("het", "components"):
        value = job.get(name)
        if isinstance(value, list):
            return _list_of_objects(value)
    return []


def _member_jobs(job):
    for name in ("array_tasks", "array_members", "tasks"):
        value = job.get(name)
        if isinstance(value, list):
            return _list_of_objects(value)
    return []


def parse_task_range(spec):
    """解析 ``1-4``、``1,3,5-7``、``0-98:2``。无效范围返回 None。"""
    text = _text(spec)
    if text is None:
        return None
    values = []
    for part in text.split(","):
        match = _TASK_TOKEN.match(part.strip())
        if match is None:
            return None
        start = int(match.group(1))
        end = int(match.group(2)) if match.group(2) is not None else start
        step = int(match.group(3)) if match.group(3) is not None else 1
        if step <= 0 or end < start:
            return None
        values.extend(range(start, end + 1, step))
    return values


def _attempt_key(cluster, job_id, submit_time, restart_cnt):
    return "|".join((cluster, str(job_id), str(submit_time), str(restart_cnt)))


class _Session:
    """一次汇合事务内的索引与写入辅助。"""

    def __init__(self, connection, received_at):
        self.connection = connection
        self.received_at = received_at
        self.attempts = {}
        for row in connection.execute(
            "SELECT * FROM attempts"
        ):
            self.attempts[row["attempt_key"]] = row

    def attempt(self, cluster, job_id, submit_time, restart_cnt, identity_status, top_level=True):
        key = _attempt_key(cluster, job_id, submit_time, restart_cnt)
        row = self.attempts.get(key)
        if row is None:
            cursor = self.connection.execute(
                "INSERT INTO attempts(attempt_key,cluster,job_id,submit_time,restart_cnt,identity_status,"
                "top_level,first_seen,last_seen) VALUES (?,?,?,?,?,?,?,?,?)",
                (key, cluster, job_id, submit_time, restart_cnt, identity_status, int(top_level),
                 self.received_at, self.received_at),
            )
            row = self.connection.execute("SELECT * FROM attempts WHERE id=?", (cursor.lastrowid,)).fetchone()
            self.attempts[key] = row
            self.event(key, "attempt_opened", {
                "cluster": cluster, "job_id": job_id, "submit_time": submit_time,
                "restart_cnt": restart_cnt, "top_level": top_level,
            })
        else:
            self.connection.execute("UPDATE attempts SET last_seen=? WHERE id=?", (self.received_at, row["id"]))
        return row

    def unresolved_attempt(self, cluster, job_id, record_id, fields):
        """身份不完整时保留独立观察，不并入任何已知尝试。"""
        key = "|".join((cluster, str(job_id), "unresolved", "record-" + str(record_id)))
        row = self.connection.execute("SELECT * FROM attempts WHERE attempt_key=?", (key,)).fetchone()
        if row is None:
            cursor = self.connection.execute(
                "INSERT INTO attempts(attempt_key,cluster,job_id,submit_time,restart_cnt,identity_status,"
                "top_level,first_seen,last_seen) VALUES (?,?,?,?,?,?,?,?,?)",
                (key, cluster, job_id, None, None, "unresolved", 1, self.received_at, self.received_at),
            )
            row = self.connection.execute("SELECT * FROM attempts WHERE id=?", (cursor.lastrowid,)).fetchone()
            self.attempts[key] = row
            self.event(key, "identity_unresolved", fields)
        return row

    def event(self, attempt_key, action, detail):
        encoded = json.dumps(detail, ensure_ascii=False, sort_keys=True)
        exists = self.connection.execute(
            "SELECT 1 FROM events WHERE attempt_key IS ? AND action=? AND detail=?",
            (attempt_key, action, encoded),
        ).fetchone()
        if exists is None:
            self.connection.execute(
                "INSERT INTO events(attempt_key,action,detail,recorded_at) VALUES (?,?,?,?)",
                (attempt_key, action, encoded, self.received_at),
            )

    def evidence(self, attempt, side, run_id, record_id, kind, label, payload):
        cursor = self.connection.execute(
            "INSERT INTO evidence(attempt_id,attempt_key,side,run_id,record_id,kind,label,payload,observed_at)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (
                attempt["id"], attempt["attempt_key"], side, run_id, record_id, kind, label,
                json.dumps(payload, ensure_ascii=False, sort_keys=True), self.received_at,
            ),
        )
        return cursor.lastrowid

    def side(self, attempt, side, run_id, record_id, state, final):
        """记录一侧的依据。

        每条来源只写自己那一行；另一侧的行永远不会被触碰。同侧只有“来源的
        新观察替代来源的旧观察”，而且旧观察留在 evidence 中可回看。终态一旦
        落定不会被非终态倒回；同侧两个不同终态并列保留并记冲突。
        """
        cancelled = 1 if state in CANCELLED_STATES else 0
        existing = self.connection.execute(
            "SELECT * FROM attempt_sides WHERE attempt_id=? AND side=?", (attempt["id"], side)
        ).fetchone()
        if existing is None:
            self.evidence(attempt, side, run_id, record_id, "state", None, {"state": state, "final": final})
            self.connection.execute(
                "INSERT INTO attempt_sides(attempt_id,side,run_id,record_id,state,final,cancelled,observed_at)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (attempt["id"], side, run_id, record_id, state, int(final), cancelled, self.received_at),
            )
            return "recorded"
        if existing["final"] and not final:
            self.evidence(attempt, side, run_id, record_id, "stale_state", None,
                          {"state": state, "final": final, "kept": existing["state"]})
            self.event(attempt["attempt_key"], "side_state_regression_ignored",
                       {"side": side, "kept": existing["state"], "seen": state})
            return "regressed"
        if existing["final"] and final and existing["state"] != state:
            # 同侧两个不同终态并列保留；先落定的解释不被到达时间抹掉。
            self.evidence(attempt, side, run_id, record_id, "conflicting_state", None,
                          {"state": state, "final": final, "kept": existing["state"]})
            self.event(attempt["attempt_key"], "side_state_conflict",
                       {"side": side, "kept": existing["state"], "seen": state})
            return "conflict"
        if existing["final"] and final and existing["run_id"] == run_id:
            return "unchanged"
        self.evidence(attempt, side, run_id, record_id, "state", None,
                      {"state": state, "final": final, "supersedes": existing["state"]})
        self.connection.execute(
            "UPDATE attempt_sides SET run_id=?,record_id=?,state=?,final=?,cancelled=?,observed_at=?"
            " WHERE attempt_id=? AND side=?",
            (run_id, record_id, state, int(final), cancelled, self.received_at, attempt["id"], side),
        )
        return "finalized" if final else "updated"

    def detail(self, attempt, side, run_id, record_id, kind, label, payload):
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO attempt_details(attempt_id,side,run_id,record_id,kind,label,detail,observed_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (
                attempt["id"], side, run_id, record_id, kind, label,
                json.dumps(payload, ensure_ascii=False, sort_keys=True), self.received_at,
            ),
        )
        if cursor.rowcount:
            self.connection.execute(
                "UPDATE attempt_details SET run_id=?,record_id=?,detail=?,observed_at=?"
                " WHERE attempt_id=? AND side=? AND kind=? AND label=?",
                (run_id, record_id, json.dumps(payload, ensure_ascii=False, sort_keys=True),
                 self.received_at, attempt["id"], side, kind, label),
            )
            self.evidence(attempt, side, run_id, record_id, kind, label, payload)
            self.event(attempt["attempt_key"], "detail_observed",
                       {"side": side, "kind": kind, "label": label})
            return True
        self.connection.execute(
            "UPDATE attempt_details SET run_id=?,record_id=?,detail=?,observed_at=?"
            " WHERE attempt_id=? AND side=? AND kind=? AND label=?",
            (run_id, record_id, json.dumps(payload, ensure_ascii=False, sort_keys=True),
             self.received_at, attempt["id"], side, kind, label),
        )
        return False

    def member(self, attempt, source_task, task_index, materialized, state):
        existing = self.connection.execute(
            "SELECT * FROM attempt_members WHERE attempt_id=? AND source_task=?",
            (attempt["id"], source_task),
        ).fetchone()
        if existing is None:
            self.connection.execute(
                "INSERT INTO attempt_members(attempt_id,source_task,task_index,materialized,observations,last_state,first_seen,last_seen)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (attempt["id"], source_task, task_index, int(materialized), 1, state,
                 self.received_at, self.received_at),
            )
            self.event(attempt["attempt_key"], "array_member_observed",
                       {"task": source_task, "materialized": materialized, "state": state})
        else:
            self.connection.execute(
                "UPDATE attempt_members SET materialized=?,observations=observations+1,last_state=?,last_seen=?"
                " WHERE attempt_id=? AND source_task=?",
                (int(materialized or existing["materialized"]), state, self.received_at,
                 attempt["id"], source_task),
            )

    def array(self, attempt, declared_range, placeholder):
        existing = self.connection.execute(
            "SELECT * FROM attempt_arrays WHERE attempt_id=?", (attempt["id"],)
        ).fetchone()
        if existing is None:
            self.connection.execute(
                "INSERT INTO attempt_arrays(attempt_id,declared_range,placeholder,first_seen) VALUES (?,?,?,?)",
                (attempt["id"], declared_range, int(placeholder), self.received_at),
            )
        elif declared_range is not None and existing["declared_range"] != declared_range:
            # 范围本身变化是来源自己的资料更新，不改写成员计数。
            self.connection.execute(
                "UPDATE attempt_arrays SET declared_range=? WHERE attempt_id=?",
                (declared_range, attempt["id"]),
            )
        self.event(attempt["attempt_key"], "array_observed",
                   {"declared_range": declared_range, "placeholder": placeholder})


def _identity(item, job):
    cluster = _text(item.get("cluster"))
    job_id = _text(job.get("job_id"))
    submit_time = _text(job.get("submit_time"))
    restart_cnt = _optional_int(job.get("restart_cnt"))
    identity_status = "identified" if submit_time is not None and restart_cnt is not None else "unresolved"
    return cluster, job_id, submit_time, restart_cnt, identity_status


def _record(connection, run_id, job, cluster, logical_job_id, role, identity_status):
    return connection.execute(
        "INSERT INTO run_records(run_id,job_id,logical_job_id,submit_time,restart_cnt,role,"
        "array_task_id,het_group_id,identity_status,detail) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            run_id, _text(job.get("job_id")), logical_job_id, _text(job.get("submit_time")),
            _optional_int(job.get("restart_cnt")), role, _text(job.get("array_task_id")),
            _text(job.get("het_group_id")), identity_status,
            json.dumps(job, ensure_ascii=False, sort_keys=True),
        ),
    ).lastrowid


def _job_role(job, job_id):
    """区分顶层作业、数组物化成员、组件作业。后两者不建顶层尝试。"""
    array_job_id = _text(job.get("array_job_id"))
    if array_job_id is not None and array_job_id != job_id:
        return "array_member", array_job_id
    if _optional_int(job.get("array_task_id")) is not None:
        return "array_member", array_job_id or job_id
    het_group = _text(job.get("het_group_id"))
    if het_group is not None and job_id is not None and het_group != job_id:
        return "component", het_group
    return "job", array_job_id or het_group or job_id


def _observe_job(session, run_id, item, job, declared_side, summary):
    cluster, job_id, submit_time, restart_cnt, identity_status = _identity(item, job)
    if cluster is None:
        raise ValueError("cluster is required")
    role, logical_job_id = _job_role(job, job_id)
    if job_id is None and role == "job":
        raise ValueError("job_id is required")
    side = normalize_side(declared_side)

    if role in ("component", "array_member"):
        # 组件与物化成员属于下级层级：挂在所属逻辑作业上，不新建顶层尝试。
        owner = _resolve_owner(session, cluster, logical_job_id, job, summary)
        record_id = _record(session.connection, run_id, job, cluster, logical_job_id, role, identity_status)
        if owner is not None:
            if role == "component":
                label = _text(job.get("het_group")) or _text(job.get("het_group_id")) or job_id or "?"
                session.detail(owner, side or "other:unspecified", run_id, record_id,
                               "component", label, _component_payload(job))
                summary["components_seen"].add((cluster, logical_job_id, label))
            else:
                task_index = _optional_int(job.get("array_task_id"))
                if task_index is not None:
                    session.member(owner, str(task_index), task_index, True, _state_of(job)[0])
                summary["members_seen"].add((cluster, logical_job_id, str(task_index)))
            _observe_children(session, owner, side, run_id, record_id, job)
        else:
            summary["owner_unresolved"] += 1
        summary["components" if role == "component" else "members"] += 1
        return owner, record_id, side, role

    record_id = _record(session.connection, run_id, job, cluster, logical_job_id, role, identity_status)
    if identity_status == "unresolved":
        attempt = session.unresolved_attempt(
            cluster, logical_job_id, record_id,
            {"job_id": job_id, "submit_time": submit_time, "restart_cnt": restart_cnt},
        )
        summary["unresolved"] += 1
        return attempt, record_id, side, role

    attempt = session.attempt(cluster, logical_job_id, submit_time, restart_cnt, identity_status)
    summary["attempts_seen"].add(attempt["attempt_key"])
    state, final = _state_of(job)
    if side is None:
        session.event(attempt["attempt_key"], "source_side_missing", {"record_id": record_id})
    else:
        session.side(attempt, side, run_id, record_id, state, final)
        summary["sides"].add((attempt["attempt_key"], side))
    declared_range = _text(job.get("array_task_string"))
    members = _member_jobs(job)
    if declared_range is not None or members:
        placeholder = declared_range is not None and not any(
            _optional_int(member.get("array_task_id")) is not None for member in members
        )
        if declared_range is not None:
            session.array(attempt, declared_range, placeholder)
            if placeholder:
                session.member(attempt, "range:" + declared_range, None, False, None)
        for position, member in enumerate(members):
            task_index = _optional_int(member.get("array_task_id"))
            if task_index is None:
                label = _text(member.get("name")) or "task@" + str(position)
                session.member(attempt, label, None, False, _state_of(member)[0])
            else:
                session.member(attempt, str(task_index), task_index, True, _state_of(member)[0])
    _observe_children(session, attempt, side, run_id, record_id, job)
    return attempt, record_id, side, role


def _resolve_owner(session, cluster, logical_job_id, job, summary):
    """为下级记录找到所属顶层尝试；绝不因下级记录而新建顶层尝试。"""
    submit_time = _text(job.get("submit_time"))
    restart_cnt = _optional_int(job.get("restart_cnt"))
    if submit_time is not None and restart_cnt is not None:
        row = session.attempts.get(_attempt_key(cluster, logical_job_id, submit_time, restart_cnt))
        if row is not None and row["top_level"]:
            return row
    matches = [
        row for row in session.attempts.values()
        if row["cluster"] == cluster and row["job_id"] == logical_job_id and row["top_level"]
    ]
    if len(matches) == 1:
        return matches[0]
    return None


def _component_payload(job):
    return {
        "job_id": job.get("job_id"),
        "het_group": job.get("het_group"),
        "het_group_id": job.get("het_group_id"),
        "state": _state_of(job)[0],
        "final": _state_of(job)[1],
        "steps": _steps(job),
    }


def _observe_children(session, attempt, side, run_id, record_id, job):
    """steps 与嵌入组件只作为下级资料，不创建顶层尝试。"""
    child_side = side or "other:unspecified"
    for position, step in enumerate(_steps(job)):
        label = _text(step.get("step_id")) or _text(step.get("name")) or "#" + str(position)
        session.detail(attempt, child_side, run_id, record_id, "step", label, step)
    for position, component in enumerate(_components(job)):
        label = _text(component.get("het_group")) or _text(component.get("job_id")) or "#" + str(position)
        session.detail(attempt, child_side, run_id, record_id, "component", label,
                       _component_payload(component))


def _ingest(connection, source, capture_id, items, received_at):
    summary = {
        "capture_id": capture_id, "source": source, "runs": 0, "records": 0,
        "attempts_seen": set(), "sides": set(), "components": 0, "unresolved": 0,
        "members": 0, "owner_unresolved": 0,
        "components_seen": set(), "members_seen": set(),
        "issues": [],
    }
    session = _Session(connection, received_at)
    if not items:
        connection.execute(
            "INSERT INTO source_runs(source,capture_id,item_index,cluster,declared_source,"
            "declared_observation,received_at,record_count,processing) VALUES (?,?,?,?,?,?,?,?,?)",
            (source, capture_id, 0, None, None, None, received_at, 0, "empty"),
        )
        session.event(None, "empty_observation",
                      {"capture_id": capture_id, "note": "no records; no cancellation derived"})
        summary["runs"] = 1
        summary["processing"] = "empty"
        return summary
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            summary["issues"].append({"item": index, "reason": "observation is not an object"})
            continue
        cluster = _text(item.get("cluster"))
        declared_side = _text(item.get("source"))
        jobs = item.get("jobs")
        if not isinstance(jobs, list):
            summary["issues"].append({"item": index, "reason": "jobs must be a list"})
            continue
        run_id = connection.execute(
            "INSERT INTO source_runs(source,capture_id,item_index,cluster,declared_source,"
            "declared_observation,received_at,record_count,processing) VALUES (?,?,?,?,?,?,?,?,?)",
            (source, capture_id, index, cluster, declared_side, _text(item.get("observation")),
             received_at, 0, "pending"),
        ).lastrowid
        accepted = 0
        item_issues = 0
        deferred = []
        for job in jobs:
            if not isinstance(job, dict):
                summary["issues"].append({"item": index, "reason": "job is not an object"})
                item_issues += 1
                continue
            if _job_role(job, _text(job.get("job_id")))[0] != "job":
                # 下级记录先挂起，等同批顶层作业落库后再归属，避免依赖出现顺序。
                deferred.append(job)
                continue
            try:
                _observe_job(session, run_id, item, job, declared_side, summary)
                accepted += 1
            except (ValueError, TypeError) as error:
                summary["issues"].append({"item": index, "reason": str(error)})
                item_issues += 1
        for job in deferred:
            try:
                _observe_job(session, run_id, item, job, declared_side, summary)
                accepted += 1
            except (ValueError, TypeError) as error:
                summary["issues"].append({"item": index, "reason": str(error)})
                item_issues += 1
        processing = "complete" if accepted and not item_issues else ("failed" if not accepted else "partial")
        connection.execute(
            "UPDATE source_runs SET record_count=?,processing=? WHERE id=?", (accepted, processing, run_id)
        )
        summary["runs"] += 1
        summary["records"] += accepted
    summary["attempts_seen"] = sorted(summary["attempts_seen"])
    summary["sides"] = sorted(summary["sides"])
    summary["components_seen"] = sorted(summary["components_seen"])
    summary["members_seen"] = sorted(summary["members_seen"])
    summary["ok"] = not summary["issues"]
    summary["processing"] = "complete" if summary["ok"] else ("failed" if summary["records"] == 0 else "partial")
    return summary


def _settle(connection, received_at):
    attempts = connection.execute(
        "SELECT * FROM attempts WHERE top_level=1 ORDER BY id"
    ).fetchall()
    result = {"attempts": 0, "settled": 0, "outstanding": 0, "conflicts": 0, "cancellations": 0}
    for attempt in attempts:
        sides = {
            row["side"]: row
            for row in connection.execute(
                "SELECT * FROM attempt_sides WHERE attempt_id=?", (attempt["id"],)
            )
        }
        owed = [side for side in SIDES if side not in sides or not sides[side]["final"]]
        settled = not owed and attempt["identity_status"] == "identified"
        conflict = 0
        if all(side in sides and sides[side]["final"] for side in SIDES):
            if sides["controller"]["state"] != sides["accounting"]["state"]:
                conflict = 1
                session_event = json.dumps(
                    {"controller": sides["controller"]["state"], "accounting": sides["accounting"]["state"]},
                    ensure_ascii=False, sort_keys=True,
                )
                exists = connection.execute(
                    "SELECT 1 FROM events WHERE attempt_key=? AND action='sides_disagree' AND detail=?",
                    (attempt["attempt_key"], session_event),
                ).fetchone()
                if exists is None:
                    connection.execute(
                        "INSERT INTO events(attempt_key,action,detail,recorded_at) VALUES (?,?,?,?)",
                        (attempt["attempt_key"], "sides_disagree", session_event, received_at),
                    )
        connection.execute(
            "INSERT INTO attempt_status(attempt_id,settled,outstanding,conflict,reconciled_at)"
            " VALUES (?,?,?,?,?) ON CONFLICT(attempt_id) DO UPDATE SET"
            " settled=excluded.settled,outstanding=excluded.outstanding,"
            " conflict=excluded.conflict,reconciled_at=excluded.reconciled_at",
            (attempt["id"], int(settled), json.dumps(owed), int(conflict), received_at),
        )
        result["attempts"] += 1
        result["settled" if settled else "outstanding"] += 1
        result["conflicts"] += conflict
        if settled:
            cancelled = [side for side in SIDES if sides[side]["cancelled"]]
            if cancelled:
                payload = json.dumps(
                    {"sides": cancelled, "states": {side: sides[side]["state"] for side in cancelled}},
                    ensure_ascii=False, sort_keys=True,
                )
                exists = connection.execute(
                    "SELECT 1 FROM events WHERE attempt_key=? AND action='cancellation_recorded' AND detail=?",
                    (attempt["attempt_key"], payload),
                ).fetchone()
                if exists is None:
                    connection.execute(
                        "INSERT INTO events(attempt_key,action,detail,recorded_at) VALUES (?,?,?,?)",
                        (attempt["attempt_key"], "cancellation_recorded", payload, received_at),
                    )
                    result["cancellations"] += 1
    return result


def _clock():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class Merge:
    """在给定归档上执行汇合、结算与补扫。"""

    def __init__(self, archive):
        self.archive = archive
        self.connection = archive.connection
        # executescript 自行提交，建表在事务之外完成。
        self.connection.executescript(SCHEMA)

    def ingest(self, source, capture_id, items, received_at=None):
        received_at = received_at or _clock()
        with self.archive.transaction():
            summary = _ingest(self.connection, source, capture_id, items, received_at)
            result = _settle(self.connection, received_at)
        summary["settlement"] = result
        return summary

    def ingest_capture(self, capture_id, source=None, received_at=None, force=False):
        """把一条已归档的响应汇入；同一条捕获重复调用是幂等的。"""
        row = self.connection.execute("SELECT * FROM captures WHERE id=?", (capture_id,)).fetchone()
        if row is None:
            raise KeyError(capture_id)
        if not force:
            done = self.connection.execute(
                "SELECT processing FROM source_runs WHERE capture_id=? LIMIT 1", (capture_id,)
            ).fetchone()
            if done is not None:
                return {"capture_id": capture_id, "skipped": True, "processing": done["processing"]}
        if row["status"] != 200:
            self.connection.execute(
                "INSERT INTO ingest_failures(capture_id,reason,recorded_at) VALUES (?,?,?)"
                " ON CONFLICT(capture_id) DO UPDATE SET reason=excluded.reason,recorded_at=excluded.recorded_at",
                (capture_id, "non-success status " + str(row["status"]), received_at or _clock()),
            )
            return {"capture_id": capture_id, "skipped": True, "status": row["status"],
                    "reason": "non-success response is not source records"}
        try:
            headers = json.loads(row["headers"])
            items = decode_records(self.archive.body(capture_id), headers.get("content-type", ""))
        except (ValueError, IndexError, TypeError) as error:
            self.connection.execute(
                "INSERT INTO ingest_failures(capture_id,reason,recorded_at) VALUES (?,?,?)"
                " ON CONFLICT(capture_id) DO UPDATE SET reason=excluded.reason,recorded_at=excluded.recorded_at",
                (capture_id, str(error), received_at or _clock()),
            )
            return {"capture_id": capture_id, "skipped": True, "reason": str(error)}
        self.connection.execute("DELETE FROM ingest_failures WHERE capture_id=?", (capture_id,))
        return self.ingest(source or row["source"], capture_id, items, received_at)

    def replay(self, received_at=None):
        """重新汇合尚未成功解释的捕获，供进程重启后补齐。"""
        rows = self.connection.execute(
            "SELECT c.id FROM captures c"
            " LEFT JOIN source_runs r ON r.capture_id=c.id"
            " WHERE c.status=200 AND r.id IS NULL ORDER BY c.id"
        ).fetchall()
        return [self.ingest_capture(row["id"], received_at=received_at) for row in rows]

    def reconcile(self, received_at=None):
        received_at = received_at or _clock()
        with self.archive.transaction():
            result = _settle(self.connection, received_at)
        return result

    def resume(self, received_at=None):
        """进程重启后的续跑入口：重放未解释捕获并重算未结项。"""
        replayed = self.replay(received_at)
        result = self.reconcile(received_at)
        result["replayed"] = len(replayed)
        result["pending"] = self.pending()
        return result

    # ---- 查询视图 ------------------------------------------------------

    def pending(self):
        """尚欠依据的尝试，按逻辑作业与尝试列出。"""
        rows = self.connection.execute(
            "SELECT a.*, s.outstanding, s.conflict FROM attempts a"
            " JOIN attempt_status s ON s.attempt_id=a.id"
            " WHERE s.settled=0 AND a.top_level=1"
            " ORDER BY a.cluster,a.job_id,a.submit_time,a.restart_cnt,a.id"
        ).fetchall()
        result = []
        for row in rows:
            sides = {
                side: self.connection.execute(
                    "SELECT state,final,observed_at,run_id,record_id FROM attempt_sides"
                    " WHERE attempt_id=? AND side=?", (row["id"], side),
                ).fetchone()
                for side in SIDES
            }
            result.append({
                "attempt_key": row["attempt_key"],
                "cluster": row["cluster"],
                "job_id": row["job_id"],
                "submit_time": row["submit_time"],
                "restart_cnt": row["restart_cnt"],
                "identity_status": row["identity_status"],
                "outstanding": json.loads(row["outstanding"]),
                "sides": {side: (dict(value) if value is not None else None) for side, value in sides.items()},
            })
        return result

    def jobs(self):
        """按逻辑作业分组：尝试、各侧状态、组件与数组进度。"""
        attempts = self.connection.execute(
            "SELECT a.*, s.settled,s.outstanding,s.conflict FROM attempts a"
            " LEFT JOIN attempt_status s ON s.attempt_id=a.id"
            " WHERE a.top_level=1"
            " ORDER BY a.cluster,a.job_id,a.submit_time,a.restart_cnt,a.id"
        ).fetchall()
        grouped = {}
        order = []
        for attempt in attempts:
            key = (attempt["cluster"], attempt["job_id"], attempt["submit_time"])
            if key not in grouped:
                grouped[key] = {"cluster": attempt["cluster"], "job_id": attempt["job_id"],
                                "submit_time": attempt["submit_time"], "attempts": []}
                order.append(key)
            grouped[key]["attempts"].append(self._attempt_summary(attempt))
        for key in order:
            job = grouped[key]
            job["open"] = any(not attempt["settled"] for attempt in job["attempts"])
            job["settled"] = all(attempt["settled"] for attempt in job["attempts"])
            job["components"], job["steps"] = self._children_for(job)
            job["array"] = self._array_for(job)
        return [grouped[key] for key in order]

    def _attempt_summary(self, attempt):
        status = self.connection.execute(
            "SELECT * FROM attempt_status WHERE attempt_id=?", (attempt["id"],)
        ).fetchone()
        sides = {}
        for row in self.connection.execute("SELECT * FROM attempt_sides WHERE attempt_id=?", (attempt["id"],)):
            sides[row["side"]] = {"state": row["state"], "final": bool(row["final"]),
                                  "cancelled": bool(row["cancelled"]), "run_id": row["run_id"],
                                  "observed_at": row["observed_at"]}
        return {
            "attempt_key": attempt["attempt_key"],
            "restart_cnt": attempt["restart_cnt"],
            "identity_status": attempt["identity_status"],
            "sides": sides,
            "settled": bool(status["settled"]) if status else False,
            "outstanding": json.loads(status["outstanding"]) if status else list(SIDES),
            "conflict": bool(status["conflict"]) if status else False,
        }

    def _children_for(self, job):
        keys = [attempt["attempt_key"] for attempt in job["attempts"]]
        labels = {"component": {}, "step": {}}
        for key in keys:
            rows = self.connection.execute(
                "SELECT kind,label,side,detail FROM attempt_details WHERE attempt_id="
                " (SELECT id FROM attempts WHERE attempt_key=?) ORDER BY kind,label", (key,),
            ).fetchall()
            for row in rows:
                if row["kind"] in labels:
                    labels[row["kind"]].setdefault(row["label"], []).append(
                        {"attempt_key": key, "side": row["side"], "detail": json.loads(row["detail"])}
                    )
        components = [{"label": label, "observations": value} for label, value in labels["component"].items()]
        steps = [{"label": label, "observations": value} for label, value in labels["step"].items()]
        return components, steps

    def _array_for(self, job):
        result = {"declared_range": None, "placeholder": False, "members": [],
                  "materialized": 0, "placeholder_entries": 0, "states": {}}
        for attempt in job["attempts"]:
            row = self.connection.execute(
                "SELECT * FROM attempt_arrays WHERE attempt_id="
                " (SELECT id FROM attempts WHERE attempt_key=?)", (attempt["attempt_key"],),
            ).fetchone()
            if row is not None:
                result["declared_range"] = row["declared_range"]
                result["placeholder"] = bool(row["placeholder"])
            members = self.connection.execute(
                "SELECT * FROM attempt_members WHERE attempt_id="
                " (SELECT id FROM attempts WHERE attempt_key=?) ORDER BY source_task",
                (attempt["attempt_key"],),
            ).fetchall()
            for member in members:
                entry = {
                    "attempt_key": attempt["attempt_key"],
                    "source_task": member["source_task"],
                    "task_index": member["task_index"],
                    "materialized": bool(member["materialized"]),
                    "observations": member["observations"],
                    "last_state": member["last_state"],
                }
                result["members"].append(entry)
                if not member["materialized"]:
                    result["placeholder_entries"] += 1
                    continue
                result["materialized"] += 1
                state = member["last_state"] or "UNKNOWN"
                result["states"][state] = result["states"].get(state, 0) + 1
        return result

    def attempt(self, attempt_key):
        row = self.connection.execute("SELECT * FROM attempts WHERE attempt_key=?", (attempt_key,)).fetchone()
        if row is None:
            raise KeyError(attempt_key)
        sides = {}
        for side in self.connection.execute("SELECT * FROM attempt_sides WHERE attempt_id=?", (row["id"],)):
            sides[side["side"]] = {
                "state": side["state"], "final": bool(side["final"]),
                "cancelled": bool(side["cancelled"]), "run_id": side["run_id"],
                "record_id": side["record_id"], "observed_at": side["observed_at"],
            }
        details = []
        for detail in self.connection.execute(
            "SELECT * FROM attempt_details WHERE attempt_id=? ORDER BY kind,side,label", (row["id"],)
        ):
            details.append({
                "kind": detail["kind"], "label": detail["label"], "side": detail["side"],
                "detail": json.loads(detail["detail"]), "run_id": detail["run_id"],
                "record_id": detail["record_id"],
            })
        members = [dict(member) for member in self.connection.execute(
            "SELECT * FROM attempt_members WHERE attempt_id=? ORDER BY source_task", (row["id"],)
        )]
        array = self.connection.execute("SELECT * FROM attempt_arrays WHERE attempt_id=?", (row["id"],)).fetchone()
        status = self.connection.execute("SELECT * FROM attempt_status WHERE attempt_id=?", (row["id"],)).fetchone()
        return {
            "attempt_key": row["attempt_key"],
            "cluster": row["cluster"],
            "job_id": row["job_id"],
            "submit_time": row["submit_time"],
            "restart_cnt": row["restart_cnt"],
            "identity_status": row["identity_status"],
            "first_seen": row["first_seen"],
            "last_seen": row["last_seen"],
            "sides": sides,
            "details": details,
            "array": ({"declared_range": array["declared_range"],
                       "placeholder": bool(array["placeholder"])} if array else None),
            "members": [{**member, "materialized": bool(member["materialized"])} for member in members],
            "settled": bool(status["settled"]) if status else False,
            "outstanding": json.loads(status["outstanding"]) if status else list(SIDES),
            "conflict": bool(status["conflict"]) if status else False,
        }

    def evidence(self, attempt_key):
        """下钻到源响应：每条依据指向的捕获及原始正文。"""
        known = self.connection.execute(
            "SELECT id FROM attempts WHERE attempt_key=?", (attempt_key,)
        ).fetchone()
        if known is None:
            raise KeyError(attempt_key)
        rows = self.connection.execute(
            "SELECT e.*, r.capture_id, c.url,c.status,c.received_at,c.digest FROM evidence e"
            " JOIN source_runs r ON r.id=e.run_id"
            " JOIN captures c ON c.id=r.capture_id"
            " WHERE e.attempt_id=? ORDER BY e.id", (known["id"],),
        ).fetchall()
        result = []
        for row in rows:
            entry = {
                "evidence_id": row["id"], "kind": row["kind"], "label": row["label"],
                "side": row["side"], "payload": json.loads(row["payload"]),
                "observed_at": row["observed_at"], "run_id": row["run_id"],
                "capture": {"capture_id": row["capture_id"], "url": row["url"],
                            "status": row["status"], "received_at": row["received_at"],
                            "digest": row["digest"]},
            }
            result.append(entry)
        return result

    def response(self, capture_id):
        """返回一条原始响应，供管理员从业务视图下钻。"""
        row = self.connection.execute("SELECT * FROM captures WHERE id=?", (capture_id,)).fetchone()
        if row is None:
            raise KeyError(capture_id)
        body = self.archive.body(capture_id)
        return {
            "capture_id": capture_id, "source": row["source"], "url": row["url"],
            "status": row["status"], "headers": row["headers"],
            "received_at": row["received_at"], "digest": row["digest"],
            "body": body.decode("utf-8", errors="replace"),
        }

    def events(self, attempt_key=None):
        query = "SELECT * FROM events"
        params = ()
        if attempt_key is not None:
            query += " WHERE attempt_key=?"
            params = (attempt_key,)
        return [
            {**dict(row), "detail": json.loads(row["detail"])}
            for row in self.connection.execute(query + " ORDER BY id", params)
        ]


def merge_capture(merge, client, source, path="/feed", received_at=None):
    """实时轮询一次：先原子归档原始响应，再在独立事务中汇合。"""
    from .client import collect
    capture_id, response = collect(client, merge.archive, source, path)
    result = {"capture_id": capture_id, "status": response.status, "path": path,
              "url": response.url, "merge": None}
    if response.status == 200:
        result["merge"] = merge.ingest_capture(capture_id, source=source, received_at=received_at)
    else:
        merge.ingest_capture(capture_id, source=source, received_at=received_at)
    return result


def backfill(merge, client, paths, source, received_at=None):
    """补扫历史地址；保留期已过的地址只记录缺口，不推导任何状态。"""
    result = []
    for path in paths:
        try:
            outcome = merge_capture(merge, client, source, path, received_at=received_at)
        except (KeyError, ValueError) as error:
            result.append({"path": path, "error": str(error)})
            continue
        entry = {"path": path, "capture_id": outcome["capture_id"], "status": outcome["status"]}
        if outcome["status"] in (404, 410):
            entry["retention"] = "expired"
        elif outcome["status"] != 200:
            entry["retention"] = "unavailable"
        elif outcome["merge"] is not None:
            entry["merge"] = {
                "records": outcome["merge"].get("records"),
                "attempts": outcome["merge"].get("attempts_seen"),
                "skipped": outcome["merge"].get("skipped", False),
            }
        result.append(entry)
    return result


def unfinished(merge):
    """补扫计划：尚欠控制器或账务终态的尝试。"""
    return [
        {"attempt_key": entry["attempt_key"], "cluster": entry["cluster"],
         "job_id": entry["job_id"], "submit_time": entry["submit_time"],
         "restart_cnt": entry["restart_cnt"], "outstanding": entry["outstanding"]}
        for entry in merge.pending()
    ]
