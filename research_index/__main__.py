import argparse
import json
import tempfile
from contextlib import contextmanager
from pathlib import Path

from .archive import Archive
from .client import Client, collect
from .decoding import records
from .jobs import JobLedger
from .origin import serve


def capture(base_url, archive_path, source, path, merge=False):
    archive = Archive(archive_path)
    try:
        capture_id, response = collect(Client(base_url), archive, source, path)
        result = {"capture_id": capture_id, "status": response.status, "bytes": len(response.body), "url": response.url}
        if response.status == 200:
            result["source_records"] = records(response.body, response.headers.get("content-type", ""))
        result["archived_observations"] = len(archive.captures())
        if merge:
            result["merge"] = JobLedger(archive).merge_capture(capture_id)
        return result
    finally:
        archive.close()


@contextmanager
def open_ledger(archive_path):
    archive = Archive(archive_path)
    try:
        yield JobLedger(archive)
    finally:
        archive.close()


def main():
    parser = argparse.ArgumentParser(description="来源响应接收、归档与作业资料汇合")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("demo")
    collector = subcommands.add_parser("capture")
    collector.add_argument("--base-url", required=True)
    collector.add_argument("--archive", type=Path, required=True)
    collector.add_argument("--source", required=True)
    collector.add_argument("--path", default="/feed")
    collector.add_argument("--merge", action="store_true", help="捕获后立即汇入作业台账")
    merger = subcommands.add_parser("reconcile", help="补扫：汇合所有尚未汇入的捕获")
    merger.add_argument("--archive", type=Path, required=True)
    listing = subcommands.add_parser("jobs", help="按逻辑作业列出汇合视图")
    listing.add_argument("--archive", type=Path, required=True)
    listing.add_argument("--cluster")
    detail = subcommands.add_parser("job", help="单个逻辑作业的尝试、组件与证据")
    detail.add_argument("--archive", type=Path, required=True)
    detail.add_argument("--cluster", required=True)
    detail.add_argument("--job-id", type=int, required=True)
    detail.add_argument("--submit-time", type=int)
    pending = subcommands.add_parser("outstanding", help="列出尚欠账务终态的尝试")
    pending.add_argument("--archive", type=Path, required=True)
    raw = subcommands.add_parser("raw", help="按 capture_id 下钻到归档的源响应")
    raw.add_argument("--archive", type=Path, required=True)
    raw.add_argument("--capture-id", type=int, required=True)
    server = subcommands.add_parser("serve")
    server.add_argument("--scenario", type=Path)
    args = parser.parse_args()
    if args.command == "demo":
        with tempfile.TemporaryDirectory() as temporary, serve() as origin:
            archive_path = Path(temporary) / "archive.sqlite3"
            result = capture(origin.base_url, archive_path, "local-source", "/feed", merge=True)
            with open_ledger(archive_path) as ledger:
                result["merged_view"] = {
                    "summary": ledger.summary(),
                    "logical_jobs": ledger.logical_jobs(),
                    "outstanding": ledger.outstanding(),
                }
            print(json.dumps({"operation": "raw_capture", **result}, ensure_ascii=False, indent=2))
    elif args.command == "capture":
        result = capture(args.base_url, args.archive, args.source, args.path, merge=args.merge)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    elif args.command == "reconcile":
        with open_ledger(args.archive) as ledger:
            results = ledger.merge_pending()
        print(json.dumps({"merged": results}, ensure_ascii=False, indent=2))
    elif args.command == "jobs":
        with open_ledger(args.archive) as ledger:
            view = {"summary": ledger.summary(), "logical_jobs": ledger.logical_jobs(args.cluster)}
        print(json.dumps(view, ensure_ascii=False, indent=2))
    elif args.command == "job":
        with open_ledger(args.archive) as ledger:
            try:
                view = ledger.job_detail(args.cluster, args.job_id, args.submit_time)
            except KeyError:
                raise SystemExit(f"logical job not found: cluster={args.cluster} job_id={args.job_id}")
            except ValueError as error:
                raise SystemExit(str(error))
        print(json.dumps(view, ensure_ascii=False, indent=2))
    elif args.command == "outstanding":
        with open_ledger(args.archive) as ledger:
            view = {"outstanding": ledger.outstanding()}
        print(json.dumps(view, ensure_ascii=False, indent=2))
    elif args.command == "raw":
        archive = Archive(args.archive)
        try:
            row = archive.connection.execute(
                "SELECT * FROM captures WHERE id=?", (args.capture_id,)).fetchone()
            if row is None:
                raise SystemExit(f"capture not found: {args.capture_id}")
            headers = json.loads(row["headers"])
            content_type = next(
                (value for name, value in headers.items() if name.lower() == "content-type"), "")
            body = archive.body(args.capture_id)
            view = {"capture": {**dict(row), "headers": headers}, "body_bytes": len(body)}
            try:
                view["records"] = records(body, content_type)
            except ValueError:
                view["records"] = None
        finally:
            archive.close()
        print(json.dumps(view, ensure_ascii=False, indent=2))
    else:
        from .origin import load_scenario
        with serve(load_scenario(args.scenario)) as origin:
            print(json.dumps({"base_url": origin.base_url}, ensure_ascii=False), flush=True)
            try:
                import threading
                threading.Event().wait()
            except KeyboardInterrupt:
                pass


if __name__ == "__main__":
    main()
