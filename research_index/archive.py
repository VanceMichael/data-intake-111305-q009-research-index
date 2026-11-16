import hashlib
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path


class Archive:
    """只追加的来源观察库。

    原始记录（bodies/captures）始终写入。业务汇合事务（ledger）失败时由调用方
    回滚，业务库的待补记录与原始归档因此各走各的事务，互不牵连。
    """

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # isolation_level=None 关闭隐式事务，原始采集的“正文+捕获”才能作为
        # 一个真正的原子单元；显式事务由 transaction() 负责。
        self.connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS bodies (
                digest TEXT PRIMARY KEY,
                content BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS captures (
                id INTEGER PRIMARY KEY,
                source TEXT NOT NULL,
                url TEXT NOT NULL,
                status INTEGER NOT NULL,
                headers TEXT NOT NULL,
                digest TEXT NOT NULL REFERENCES bodies(digest),
                received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );
        """)

    def close(self):
        self.connection.close()

    @contextmanager
    def transaction(self):
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield self.connection
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise
        else:
            self.connection.execute("COMMIT")

    def append(self, source, url, status, headers, body):
        if not isinstance(source, str) or not source.strip():
            raise ValueError("source is required")
        if not isinstance(body, bytes):
            raise TypeError("body must be bytes")
        if isinstance(status, bool) or not isinstance(status, int) or not 100 <= status <= 599:
            raise ValueError("invalid HTTP status")
        digest = hashlib.sha256(body).hexdigest()
        encoded_headers = json.dumps(dict(headers), ensure_ascii=False, sort_keys=True)
        with self.transaction() as connection:
            connection.execute("INSERT OR IGNORE INTO bodies VALUES (?, ?)", (digest, body))
            cursor = connection.execute(
                "INSERT INTO captures(source,url,status,headers,digest) VALUES (?,?,?,?,?)",
                (source, url, status, encoded_headers, digest),
            )
        return cursor.lastrowid

    def captures(self, source=None):
        query = "SELECT * FROM captures"
        params = ()
        if source is not None:
            query += " WHERE source=?"
            params = (source,)
        rows = self.connection.execute(query + " ORDER BY id", params).fetchall()
        return [{**dict(row), "headers": json.loads(row["headers"])} for row in rows]

    def body(self, capture_id):
        row = self.connection.execute(
            "SELECT b.digest,b.content FROM bodies b JOIN captures c ON c.digest=b.digest WHERE c.id=?",
            (capture_id,),
        ).fetchone()
        if row is None:
            raise KeyError(capture_id)
        content = bytes(row["content"])
        if hashlib.sha256(content).hexdigest() != row["digest"]:
            raise ValueError("archived content digest mismatch")
        return content
