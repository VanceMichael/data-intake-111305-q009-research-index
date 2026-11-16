import argparse
import json
import tempfile
from pathlib import Path

from .archive import Archive
from .client import Client, collect
from .decoding import records
from .merge import Merge, backfill, merge_capture, unfinished
from .origin import serve


def capture(base_url, archive_path, source, path):
    archive = Archive(archive_path)
    try:
        capture_id, response = collect(Client(base_url), archive, source, path)
        result = {"capture_id": capture_id, "status": response.status, "bytes": len(response.body), "url": response.url}
        if response.status == 200:
            result["source_records"] = records(response.body, response.headers.get("content-type", ""))
        result["archived_observations"] = len(archive.captures())
        return result
    finally:
        archive.close()


def merged_capture(base_url, archive_path, source, path):
    archive = Archive(archive_path)
    try:
        return merge_capture(Merge(archive), Client(base_url), source, path)
    finally:
        archive.close()


def query(archive_path, operation):
    archive = Archive(archive_path)
    try:
        merge = Merge(archive)
        if operation == "jobs":
            return merge.jobs()
        if operation == "pending":
            return merge.pending()
        if operation == "resume":
            return merge.resume()
        raise ValueError("unknown operation")
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
    merged = subcommands.add_parser("merge")
    merged.add_argument("--base-url", required=True)
    merged.add_argument("--archive", type=Path, required=True)
    merged.add_argument("--source", required=True)
    merged.add_argument("--path", action="append", default=None)
    server = subcommands.add_parser("serve")
    server.add_argument("--scenario", type=Path)
    for name in ("jobs", "pending", "resume"):
        view = subcommands.add_parser(name)
        view.add_argument("--archive", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "demo":
        with tempfile.TemporaryDirectory() as temporary, serve() as origin:
            result = capture(origin.base_url, Path(temporary) / "archive.sqlite3", "local-source", "/feed")
            print(json.dumps({"operation": "raw_capture", **result}, ensure_ascii=False, indent=2))
    elif args.command == "capture":
        print(json.dumps(capture(args.base_url, args.archive, args.source, args.path), ensure_ascii=False, indent=2))
    elif args.command == "merge":
        archive = Archive(args.archive)
        try:
            merge = Merge(archive)
            client = Client(args.base_url)
            paths = args.path or ["/feed"]
            output = {
                "merged": [merge_capture(merge, client, args.source, path) for path in paths],
                "unfinished": unfinished(merge),
            }
            print(json.dumps(output, ensure_ascii=False, indent=2))
        finally:
            archive.close()
    elif args.command in ("jobs", "pending", "resume"):
        print(json.dumps(query(args.archive, args.command), ensure_ascii=False, indent=2))
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
