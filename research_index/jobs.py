"""作业资料汇合：控制器与账务观察按逻辑作业、尝试与组件归并。

原始响应保留在归档库的 captures 中，本模块只在其上维护可重建的汇合视图：

- 逻辑作业以 (cluster, job_id, submit_time) 标识，job_id 不单独充当全局键；
- 重启尝试按 restart_cnt 分层，旧尝试的终态不覆盖新尝试；
- 控制器与账务的证据分别保留，两侧暂时不一致时不按到达时间互相覆盖；
- 数组占位范围不物化为任务，异构组件与 steps 不重复计为顶层作业；
- 查询无记录不产生任何事件；未结项持久保存，进程重启后可继续补齐。
"""

import hashlib
import json

# 账务终态：出现其中任一状态即视为该尝试已结清账务依据。
TERMINAL_STATES = frozenset({
    "BOOT_FAIL", "CANCELLED", "COMPLETED", "DEADLINE", "END", "FAILED",
    "LAUNCH_FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "PREEMPTED", "REQUEUED",
    "REVOKED", "SPECIAL_EXIT", "STOPPED", "TERMINATED", "TIMEOUT",
})

# 运行中状态：用于判断一次尝试是否真正运行过。
ACTIVE_STATES = frozenset({
    "COMPLETING", "CONFIGURING", "RESIZING", "RUNNING", "SIGNALING",
    "STAGE_OUT", "SUSPENDED",
})

# 计入顶层任务的逻辑作业角色；占位范围与异构组件不在其列。
COUNTED_ROLES = frozenset({"standalone", "array_member", "het_leader"})

_SCHEMA = """
CREATE TABLE IF NOT EXISTS logical_jobs (
    id INTEGER PRIMARY KEY,
    cluster TEXT NOT NULL,
    job_id INTEGER NOT NULL,
    submit_time INTEGER,
    role TEXT NOT NULL,
    array_job_id INTEGER,
    array_task_id INTEGER,
    array_task_string TEXT,
    het_job_id INTEGER,
    het_job_offset INTEGER,
    first_capture_id INTEGER NOT NULL REFERENCES captures(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS logical_jobs_identity
    ON logical_jobs(cluster, job_id, COALESCE(submit_time, -1));
CREATE TABLE IF NOT EXISTS attempts (
    id INTEGER PRIMARY KEY,
    logical_job_id INTEGER NOT NULL REFERENCES logical_jobs(id),
    restart_cnt INTEGER NOT NULL,
    UNIQUE(logical_job_id, restart_cnt)
);
CREATE TABLE IF NOT EXISTS evidence (
    id INTEGER PRIMARY KEY,
    attempt_id INTEGER NOT NULL REFERENCES attempts(id),
    source TEXT NOT NULL,
    observation INTEGER,
    capture_id INTEGER NOT NULL REFERENCES captures(id),
    state TEXT NOT NULL,
    detail TEXT NOT NULL,
    detail_digest TEXT NOT NULL,
    received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE(attempt_id, source, detail_digest)
);
CREATE INDEX IF NOT EXISTS evidence_attempt ON evidence(attempt_id);
CREATE TABLE IF NOT EXISTS attempt_steps (
    id INTEGER PRIMARY KEY,
    attempt_id INTEGER NOT NULL REFERENCES attempts(id),
    evidence_id INTEGER NOT NULL REFERENCES evidence(id),
    step_id TEXT NOT NULL,
    exit_code INTEGER,
    detail TEXT NOT NULL,
    UNIQUE(attempt_id, evidence_id, step_id)
);
CREATE TABLE IF NOT EXISTS merge_inputs (
    capture_id INTEGER PRIMARY KEY REFERENCES captures(id),
    records_seen INTEGER NOT NULL,
    jobs_seen INTEGER NOT NULL,
    merged_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
"""


def _is_terminal(states):
    return any(state.upper() in TERMINAL_STATES for state in states)


def _require_text(record, name):
    value = record.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(name + " must be non-empty text")
    return value.strip()


def _optional_int(record, name):
    value = record.get(name)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(name + " must be an integer")
    return value


def _state_list(value):
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return list(value)
    raise ValueError("state must be a string or a list of strings")


def _classify(job):
    """按数组与异构字段判定记录角色，并保留全部归属链接。"""
    array_job_id = _optional_int(job, "array_job_id")
    array_task_id = _optional_int(job, "array_task_id")
    array_task_string = job.get("array_task_string")
    if array_task_string is not None:
        if not isinstance(array_task_string, str) or not array_task_string.strip():
            raise ValueError("array_task_string must be non-empty text")
        array_task_string = array_task_string.strip()
    het_job_id = _optional_int(job, "het_job_id")
    het_job_offset = _optional_int(job, "het_job_offset")
    if het_job_offset is not None and het_job_offset < 0:
        raise ValueError("het_job_offset must be non-negative")
    if array_task_id is not None:
        role = "array_member"
    elif array_task_string is not None:
        role = "array_placeholder"
    elif het_job_offset:
        role = "het_component"
    elif het_job_id is not None:
        role = "het_leader"
    else:
        role = "standalone"
    links = {
        "array_job_id": array_job_id,
        "array_task_id": array_task_id,
        "array_task_string": array_task_string,
        "het_job_id": het_job_id,
        "het_job_offset": het_job_offset,
    }
    return role, links


def _latest_per_source(rows):
    """同一来源内按来源自报 observation 序号识别新观察，缺序号时按到达顺序。"""
    latest = {}
    for row in rows:
        key = (row["observation"] if row["observation"] is not None else -1, row["id"])
        current = latest.get(row["source"])
        if current is None or key >= current[0]:
            latest[row["source"]] = (key, row)
    return {source: row for source, (_, row) in latest.items()}


class JobLedger:
    """在归档库之上维护可重建的作业汇合台账。"""

    def __init__(self, archive):
        self.archive = archive
        self.connection = archive.connection
        self.connection.executescript(_SCHEMA)

    # ---- 汇入 ----

    def merge_capture(self, capture_id):
        """汇合一条已归档捕获；重复执行幂等。单个捕获在一个事务内完成。"""
        with self.archive.transaction():
            merged = self.connection.execute(
                "SELECT records_seen, jobs_seen FROM merge_inputs WHERE capture_id=?",
                (capture_id,)).fetchone()
            if merged is not None:
                return {"capture_id": capture_id, "records_seen": merged["records_seen"],
                        "jobs_seen": merged["jobs_seen"], "skipped": True}
            row = self.connection.execute(
                "SELECT * FROM captures WHERE id=?", (capture_id,)).fetchone()
            if row is None:
                raise KeyError(capture_id)
            if row["status"] != 200:
                self._mark_merged(capture_id, 0, 0)
                return {"capture_id": capture_id, "records_seen": 0, "jobs_seen": 0, "skipped": False}
            headers = json.loads(row["headers"])
            content_type = next(
                (value for name, value in headers.items() if name.lower() == "content-type"), "")
            from .decoding import records
            envelopes = records(self.archive.body(capture_id), content_type)
            jobs_seen = 0
            for envelope in envelopes:
                jobs_seen += self._merge_envelope(capture_id, envelope)
            self._mark_merged(capture_id, len(envelopes), jobs_seen)
            return {"capture_id": capture_id, "records_seen": len(envelopes),
                    "jobs_seen": jobs_seen, "skipped": False}

    def merge_pending(self):
        """补扫：汇合所有尚未汇入的捕获。单个失败不阻塞其余，错误随结果返回。"""
        merged_ids = {row["capture_id"] for row in self.connection.execute(
            "SELECT capture_id FROM merge_inputs")}
        results = []
        for capture in self.archive.captures():
            if capture["id"] in merged_ids:
                continue
            try:
                results.append(self.merge_capture(capture["id"]))
            except Exception as error:
                results.append({"capture_id": capture["id"], "error": str(error)})
        return results

    def _mark_merged(self, capture_id, records_seen, jobs_seen):
        self.connection.execute(
            "INSERT INTO merge_inputs(capture_id, records_seen, jobs_seen) VALUES (?,?,?)",
            (capture_id, records_seen, jobs_seen))

    def _merge_envelope(self, capture_id, envelope):
        cluster = _require_text(envelope, "cluster")
        source = _require_text(envelope, "source")
        observation = _optional_int(envelope, "observation")
        jobs = envelope.get("jobs", [])
        if not isinstance(jobs, list) or any(not isinstance(job, dict) for job in jobs):
            raise ValueError("jobs must be a list of objects")
        for job in jobs:
            try:
                self._merge_job(capture_id, cluster, source, observation, job)
            except ValueError as error:
                raise ValueError(
                    f"invalid job record from {cluster}/{source} observation "
                    f"{observation}: {error}") from error
        return len(jobs)

    def _merge_job(self, capture_id, cluster, source, observation, job):
        job_id = _optional_int(job, "job_id")
        if job_id is None:
            raise ValueError("job_id must be an integer")
        submit_time = _optional_int(job, "submit_time")
        restart_cnt = _optional_int(job, "restart_cnt")
        if restart_cnt is None:
            restart_cnt = 0
        if restart_cnt < 0:
            raise ValueError("restart_cnt must be non-negative")
        state = _state_list(job.get("state"))
        role, links = _classify(job)
        logical_id = self._upsert_logical_job(cluster, job_id, submit_time, role, links, capture_id)
        attempt_id = self._upsert_attempt(logical_id, restart_cnt)
        evidence_id = self._insert_evidence(attempt_id, source, observation, capture_id, state, job)
        if evidence_id is not None:
            self._insert_steps(attempt_id, evidence_id, job.get("steps", []))

    def _upsert_logical_job(self, cluster, job_id, submit_time, role, links, capture_id):
        row = self.connection.execute(
            "SELECT * FROM logical_jobs WHERE cluster=? AND job_id=?"
            " AND COALESCE(submit_time, -1)=COALESCE(?, -1)",
            (cluster, job_id, submit_time)).fetchone()
        if row is None:
            cursor = self.connection.execute(
                "INSERT INTO logical_jobs(cluster, job_id, submit_time, role,"
                " array_job_id, array_task_id, array_task_string, het_job_id,"
                " het_job_offset, first_capture_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (cluster, job_id, submit_time, role, links["array_job_id"],
                 links["array_task_id"], links["array_task_string"],
                 links["het_job_id"], links["het_job_offset"], capture_id))
            return cursor.lastrowid
        # 只填补未知归属，不用后到的缺失字段抹掉已知信息。
        updates = {name: value for name, value in links.items()
                   if row[name] is None and value is not None}
        if row["role"] == "standalone" and role != "standalone":
            updates["role"] = role
        if updates:
            assignments = ", ".join(name + "=?" for name in updates)
            self.connection.execute(
                f"UPDATE logical_jobs SET {assignments} WHERE id=?",
                (*updates.values(), row["id"]))
        return row["id"]

    def _upsert_attempt(self, logical_id, restart_cnt):
        self.connection.execute(
            "INSERT OR IGNORE INTO attempts(logical_job_id, restart_cnt) VALUES (?,?)",
            (logical_id, restart_cnt))
        return self.connection.execute(
            "SELECT id FROM attempts WHERE logical_job_id=? AND restart_cnt=?",
            (logical_id, restart_cnt)).fetchone()["id"]

    def _insert_evidence(self, attempt_id, source, observation, capture_id, state, job):
        canonical = json.dumps(job, ensure_ascii=False, sort_keys=True)
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO evidence(attempt_id, source, observation, capture_id,"
            " state, detail, detail_digest) VALUES (?,?,?,?,?,?,?)",
            (attempt_id, source, observation, capture_id,
             json.dumps(state, ensure_ascii=False), canonical, digest))
        return cursor.lastrowid if cursor.rowcount else None

    def _insert_steps(self, attempt_id, evidence_id, steps):
        if not isinstance(steps, list) or any(not isinstance(step, dict) for step in steps):
            raise ValueError("steps must be a list of objects")
        for step in steps:
            if "step_id" not in step:
                raise ValueError("step missing step_id")
            exit_code = step.get("exit_code")
            if isinstance(exit_code, bool) or not isinstance(exit_code, int):
                exit_code = None
            self.connection.execute(
                "INSERT OR IGNORE INTO attempt_steps(attempt_id, evidence_id, step_id,"
                " exit_code, detail) VALUES (?,?,?,?,?)",
                (attempt_id, evidence_id, str(step["step_id"]), exit_code,
                 json.dumps(step, ensure_ascii=False, sort_keys=True)))

    # ---- 视图 ----

    def logical_jobs(self, cluster=None):
        query = "SELECT * FROM logical_jobs"
        params = ()
        if cluster is not None:
            query += " WHERE cluster=?"
            params = (cluster,)
        rows = self.connection.execute(
            query + " ORDER BY cluster, job_id, COALESCE(submit_time, -1)", params).fetchall()
        return [self._job_view(row) for row in rows]

    def job_detail(self, cluster, job_id, submit_time=None):
        """按逻辑作业展示尝试、组件与证据；submit_time 缺省时要求编号唯一。"""
        if submit_time is None:
            rows = self.connection.execute(
                "SELECT * FROM logical_jobs WHERE cluster=? AND job_id=?"
                " ORDER BY COALESCE(submit_time, -1)", (cluster, job_id)).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM logical_jobs WHERE cluster=? AND job_id=?"
                " AND COALESCE(submit_time, -1)=?", (cluster, job_id, submit_time)).fetchall()
        if not rows:
            raise KeyError((cluster, job_id, submit_time))
        if len(rows) > 1:
            candidates = ", ".join(str(row["submit_time"]) for row in rows)
            raise ValueError(
                f"job_id {job_id} has multiple logical jobs on {cluster};"
                f" pick submit_time from: {candidates}")
        return self._job_view(rows[0], with_evidence=True)

    def outstanding(self):
        """尚欠账务终态的尝试；只依据已存证据判断，不凭空推断。"""
        pending = []
        rows = self.connection.execute(
            "SELECT a.id AS attempt_id, a.restart_cnt, l.cluster, l.job_id,"
            " l.submit_time, l.role FROM attempts a"
            " JOIN logical_jobs l ON l.id = a.logical_job_id"
            " ORDER BY l.cluster, l.job_id, a.restart_cnt").fetchall()
        for row in rows:
            evidence = self.connection.execute(
                "SELECT * FROM evidence WHERE attempt_id=? ORDER BY id",
                (row["attempt_id"],)).fetchall()
            if any(item["source"] == "accounting" and _is_terminal(json.loads(item["state"]))
                   for item in evidence):
                continue
            sources = _latest_per_source(evidence)
            states = [state for item in evidence for state in json.loads(item["state"])]
            controller = sources.get("controller")
            accounting = sources.get("accounting")
            pending.append({
                "cluster": row["cluster"],
                "job_id": row["job_id"],
                "submit_time": row["submit_time"],
                "restart_cnt": row["restart_cnt"],
                "role": row["role"],
                "controller_state": json.loads(controller["state"]) if controller else None,
                "accounting_state": json.loads(accounting["state"]) if accounting else None,
                "ran": any(state.upper() in ACTIVE_STATES or state.upper() in TERMINAL_STATES
                           for state in states),
                "first_seen": min(item["received_at"] for item in evidence),
                "last_seen": max(item["received_at"] for item in evidence),
                "captures": sorted({item["capture_id"] for item in evidence}),
            })
        return pending

    def summary(self):
        roles = {row["role"]: row["n"] for row in self.connection.execute(
            "SELECT role, COUNT(*) AS n FROM logical_jobs GROUP BY role")}
        attempts = self.connection.execute("SELECT COUNT(*) AS n FROM attempts").fetchone()["n"]
        return {
            "logical_jobs": sum(roles.values()),
            "counted_tasks": sum(roles.get(role, 0) for role in COUNTED_ROLES),
            "array_placeholders": roles.get("array_placeholder", 0),
            "array_members": roles.get("array_member", 0),
            "het_components": roles.get("het_component", 0),
            "attempts": attempts,
            "unsettled_attempts": len(self.outstanding()),
        }

    # ---- 视图组装 ----

    def _job_view(self, row, with_evidence=False):
        attempts = self.connection.execute(
            "SELECT * FROM attempts WHERE logical_job_id=? ORDER BY restart_cnt",
            (row["id"],)).fetchall()
        view = {
            "cluster": row["cluster"],
            "job_id": row["job_id"],
            "submit_time": row["submit_time"],
            "role": row["role"],
            "counts_as_task": row["role"] in COUNTED_ROLES,
            "array": self._link_view(row, "array_job_id", "array_task_id", "array_task_string"),
            "het": self._link_view(row, "het_job_id", "het_job_offset"),
            "attempts": [self._attempt_view(attempt, with_evidence) for attempt in attempts],
        }
        if with_evidence:
            if row["array_job_id"] is not None or row["array_task_string"] is not None:
                view["members"] = self._members(row)
            if row["het_job_id"] is not None:
                view["components"] = self._components(row)
        return view

    @staticmethod
    def _link_view(row, *names):
        if all(row[name] is None for name in names):
            return None
        return {name: row[name] for name in names}

    def _members(self, row):
        array_key = row["array_job_id"] if row["array_job_id"] is not None else row["job_id"]
        rows = self.connection.execute(
            "SELECT cluster, job_id, submit_time, array_task_id, role FROM logical_jobs"
            " WHERE cluster=? AND array_job_id=? AND array_task_id IS NOT NULL AND id<>?"
            " ORDER BY array_task_id", (row["cluster"], array_key, row["id"])).fetchall()
        return [dict(row) for row in rows]

    def _components(self, row):
        het_key = row["het_job_id"] if row["het_job_id"] is not None else row["job_id"]
        rows = self.connection.execute(
            "SELECT cluster, job_id, submit_time, het_job_offset, role FROM logical_jobs"
            " WHERE cluster=? AND het_job_id=? AND het_job_offset > 0 AND id<>?"
            " ORDER BY het_job_offset", (row["cluster"], het_key, row["id"])).fetchall()
        return [dict(row) for row in rows]

    def _attempt_view(self, attempt, with_evidence=False):
        rows = self.connection.execute(
            "SELECT * FROM evidence WHERE attempt_id=? ORDER BY id", (attempt["id"],)).fetchall()
        sources = _latest_per_source(rows)
        view = {
            "restart_cnt": attempt["restart_cnt"],
            "sources": {source: self._evidence_brief(row) for source, row in sources.items()},
            "settled": any(row["source"] == "accounting" and _is_terminal(json.loads(row["state"]))
                           for row in rows),
            "steps": self._steps_view(attempt["id"]),
        }
        if with_evidence:
            view["evidence"] = [self._evidence_view(row) for row in rows]
        return view

    @staticmethod
    def _evidence_brief(row):
        return {
            "state": json.loads(row["state"]),
            "observation": row["observation"],
            "capture_id": row["capture_id"],
            "received_at": row["received_at"],
        }

    def _evidence_view(self, row):
        return {
            "id": row["id"],
            "source": row["source"],
            **self._evidence_brief(row),
            "detail": json.loads(row["detail"]),
        }

    def _steps_view(self, attempt_id):
        rows = self.connection.execute(
            "SELECT s.*, e.observation FROM attempt_steps s"
            " JOIN evidence e ON e.id = s.evidence_id WHERE s.attempt_id=?"
            " ORDER BY s.id", (attempt_id,)).fetchall()
        latest = {}
        for row in rows:
            key = (row["observation"] if row["observation"] is not None else -1,
                   row["evidence_id"])
            current = latest.get(row["step_id"])
            if current is None or key >= current[0]:
                latest[row["step_id"]] = (key, row)
        return [
            {"step_id": row["step_id"], "exit_code": row["exit_code"],
             "evidence_id": row["evidence_id"], "detail": json.loads(row["detail"])}
            for _, row in (latest[step_id] for step_id in sorted(latest))
        ]
