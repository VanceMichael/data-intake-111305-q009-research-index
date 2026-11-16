import json
import tempfile
import unittest
from pathlib import Path

from research_index.archive import Archive
from research_index.client import Client
from research_index.merge import (
    Merge, backfill, merge_capture, parse_task_range, unfinished,
)
from research_index.origin import serve


def observation(cluster="science-a", source="controller", version=1, jobs=None):
    return {"cluster": cluster, "source": source, "observation": version, "jobs": jobs or []}


def job(job_id=71, submit=1000, restart=0, state="RUNNING", **extra):
    record = {"job_id": job_id, "submit_time": submit, "restart_cnt": restart, "state": [state]}
    record.update(extra)
    return record


class MergeCase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.archive = Archive(Path(self.temporary.name) / "merge.sqlite3")
        self.addCleanup(self.archive.close)
        self.merge = Merge(self.archive)

    def feed(self, items, source="fixture", status=200, content_type="application/json", raw=None):
        body = raw if raw is not None else json.dumps({"items": items}, ensure_ascii=False).encode()
        capture_id = self.archive.append(source, "/feed", status, {"content-type": content_type}, body)
        return self.merge.ingest_capture(capture_id)

    def attempt(self, key):
        return self.merge.attempt(key)


class IdentityTests(MergeCase):
    def test_same_job_id_on_two_clusters_is_two_logical_jobs(self):
        self.feed([observation("science-a", jobs=[job()])])
        self.feed([observation("science-b", jobs=[job()])])
        jobs = self.merge.jobs()
        self.assertEqual([(entry["cluster"], entry["job_id"]) for entry in jobs],
                         [("science-a", "71"), ("science-b", "71")])
        keys = {entry["attempts"][0]["attempt_key"] for entry in jobs}
        self.assertEqual(len(keys), 2)

    def test_restart_attempt_is_not_a_second_job(self):
        self.feed([
            observation(version=1, jobs=[job(restart=0, state="RUNNING")]),
            observation(version=2, jobs=[job(restart=1, state="PENDING")]),
        ])
        jobs = self.merge.jobs()
        self.assertEqual(len(jobs), 1)
        self.assertEqual([entry["restart_cnt"] for entry in jobs[0]["attempts"]], [0, 1])
        self.assertEqual(jobs[0]["attempts"][0]["sides"]["controller"]["state"], "RUNNING")
        self.assertEqual(jobs[0]["attempts"][1]["sides"]["controller"]["state"], "PENDING")

    def test_missing_identity_is_kept_apart_not_merged(self):
        self.feed([observation(jobs=[job()])])
        self.feed([observation(version=2, jobs=[{"job_id": 71, "state": ["FAILED"], "submit_time": 1000}])])
        jobs = self.merge.jobs()
        keys = [attempt["attempt_key"] for entry in jobs for attempt in entry["attempts"]]
        self.assertTrue(any("unresolved" in key for key in keys))
        unresolved = [key for key in keys if "unresolved" in key]
        self.assertEqual(len(unresolved), 1)
        self.assertEqual(self.attempt(unresolved[0])["identity_status"], "unresolved")
        # 不完整记录不能改写完整尝试的控制器依据。
        identified = [entry for entry in jobs if entry["attempts"][0]["identity_status"] == "identified"]
        self.assertEqual(len(identified), 1)
        self.assertEqual(identified[0]["attempts"][0]["sides"]["controller"]["state"], "RUNNING")

    def test_new_submit_time_is_a_new_logical_job(self):
        self.feed([observation(jobs=[job(submit=1000)])])
        self.feed([observation(version=2, jobs=[job(submit=2000)])])
        self.assertEqual(len(self.merge.jobs()), 2)


class SideTests(MergeCase):
    def test_late_accounting_keeps_controller_basis_until_it_settles(self):
        self.feed([observation(source="controller", jobs=[job(state="RUNNING")])])
        pending = self.merge.pending()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["outstanding"], ["controller", "accounting"])
        self.assertEqual(pending[0]["sides"]["controller"]["state"], "RUNNING")
        self.assertIsNone(pending[0]["sides"]["accounting"])

        self.feed([observation(source="accounting", version=2,
                               jobs=[job(state="FAILED", steps=[{"step_id": "batch", "exit_code": 1}])])])
        pending = self.merge.pending()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["outstanding"], ["controller"])
        self.assertEqual(pending[0]["sides"]["accounting"]["state"], "FAILED")
        self.assertEqual(pending[0]["sides"]["controller"]["state"], "RUNNING")
        # 账务依据已落定，控制器终态仍欠。
        self.assertEqual(pending[0]["sides"]["accounting"]["final"], 1)

    def test_accounting_first_then_controller_does_not_overwrite(self):
        self.feed([observation(source="accounting", jobs=[job(state="COMPLETED")])])
        self.feed([observation(source="controller", version=2, jobs=[job(state="RUNNING")])])
        attempt = self.merge.jobs()[0]["attempts"][0]
        self.assertEqual(attempt["sides"]["accounting"]["state"], "COMPLETED")
        self.assertEqual(attempt["sides"]["controller"]["state"], "RUNNING")
        # 两来源状态不同，运行被记为尚缺控制器终态而不是互相覆盖。
        self.assertIn("controller", attempt["outstanding"])
        self.assertEqual(self.merge.pending()[0]["attempt_key"], attempt["attempt_key"])

    def test_non_terminal_update_on_one_side_is_not_other_side_override(self):
        self.feed([observation(source="accounting", jobs=[job(state="FAILED")])])
        self.feed([observation(source="controller", version=2, jobs=[job(state="PENDING")])])
        self.feed([observation(source="controller", version=3, jobs=[job(state="RUNNING")])])
        attempt = self.merge.jobs()[0]["attempts"][0]
        self.assertEqual(attempt["sides"]["controller"]["state"], "RUNNING")
        self.assertEqual(attempt["sides"]["accounting"]["state"], "FAILED")
        evidence = {entry["kind"] for entry in self.merge.evidence(attempt["attempt_key"])}
        self.assertIn("state", evidence)

    def test_terminal_state_is_not_rolled_back_without_new_terminal(self):
        self.feed([observation(source="controller", jobs=[job(state="COMPLETED")])])
        self.feed([observation(source="controller", version=2, jobs=[job(state="RUNNING")])])
        attempt = self.merge.jobs()[0]["attempts"][0]
        self.assertEqual(attempt["sides"]["controller"]["state"], "COMPLETED")
        actions = [event["action"] for event in self.merge.events(attempt["attempt_key"])]
        self.assertIn("side_state_regression_ignored", actions)

    def test_both_sides_terminal_and_equal_settles_once(self):
        self.feed([observation(source="controller", jobs=[job(state="FAILED")])])
        self.feed([observation(source="accounting", version=2, jobs=[job(state="FAILED")])])
        attempt = self.merge.jobs()[0]["attempts"][0]
        self.assertTrue(attempt["settled"])
        self.assertEqual(attempt["outstanding"], [])
        self.assertFalse(attempt["conflict"])
        self.merge.reconcile()
        self.assertEqual(len(self.merge.pending()), 0)
        settled_events = [e for e in self.merge.events(attempt["attempt_key"])
                          if e["action"] == "cancellation_recorded"]
        self.assertEqual(settled_events, [])

    def test_disagreeing_terminal_states_are_kept_as_conflict(self):
        self.feed([observation(source="controller", jobs=[job(state="COMPLETED")])])
        self.feed([observation(source="accounting", version=2, jobs=[job(state="FAILED")])])
        attempt = self.merge.jobs()[0]["attempts"][0]
        self.assertTrue(attempt["conflict"])
        self.assertEqual(attempt["sides"]["controller"]["state"], "COMPLETED")
        self.assertEqual(attempt["sides"]["accounting"]["state"], "FAILED")
        self.assertIn("sides_disagree", [event["action"] for event in self.merge.events()])


class ArrayTests(MergeCase):
    def test_placeholder_range_is_not_a_launched_task(self):
        self.feed([observation(jobs=[job(state="RUNNING", array_job_id=71, array_task_string="1-4")])])
        array = self.merge.jobs()[0]["array"]
        self.assertTrue(array["placeholder"])
        self.assertEqual(array["declared_range"], "1-4")
        self.assertEqual(array["materialized"], 0)
        self.assertEqual(array["states"], {})
        self.assertEqual(array["placeholder_entries"], 1)

    def test_materialized_members_are_counted_individually(self):
        self.feed([observation(jobs=[job(
            state="RUNNING", array_job_id=71, array_task_string="1-4",
            array_tasks=[{"array_task_id": 1, "state": ["COMPLETED"]},
                         {"array_task_id": 2, "state": ["RUNNING"]}],
        )])])
        array = self.merge.jobs()[0]["array"]
        self.assertFalse(array["placeholder"])
        self.assertEqual(array["materialized"], 2)
        self.assertEqual(array["states"], {"COMPLETED": 1, "RUNNING": 1})

    def test_standalone_array_members_do_not_add_top_level_jobs(self):
        self.feed([observation(jobs=[
            job(state="RUNNING", array_job_id=71, array_task_string="1-4"),
            job(job_id=72, array_job_id=71, array_task_id=1, state="COMPLETED"),
        ])])
        jobs = self.merge.jobs()
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["job_id"], "71")
        self.assertEqual(jobs[0]["array"]["materialized"], 1)
        self.assertEqual(jobs[0]["attempts"][0]["sides"]["controller"]["state"], "RUNNING")

    def test_range_parsing(self):
        self.assertEqual(parse_task_range("1-4"), [1, 2, 3, 4])
        self.assertEqual(parse_task_range("0-6:2"), [0, 2, 4, 6])
        self.assertEqual(parse_task_range("1,3,5-6"), [1, 3, 5, 6])
        self.assertIsNone(parse_task_range("1-"))
        self.assertIsNone(parse_task_range(""))

    def test_member_states_do_not_become_top_level_final_state(self):
        self.feed([observation(jobs=[job(
            state="RUNNING", array_job_id=71, array_task_string="1-2",
            array_tasks=[{"array_task_id": 1, "state": ["COMPLETED"]}],
        )])])
        attempt = self.merge.jobs()[0]["attempts"][0]
        self.assertEqual(attempt["sides"]["controller"]["state"], "RUNNING")
        self.assertFalse(attempt["settled"])


class ComponentTests(MergeCase):
    def test_het_component_and_steps_are_children_not_top_level(self):
        self.feed([observation(source="accounting", jobs=[job(
            state="FAILED",
            steps=[{"step_id": "batch", "exit_code": 1}],
            het=[{"het_group": "g0", "job_id": 74, "state": ["FAILED"]}],
        )])])
        self.feed([observation(source="accounting", version=2, jobs=[job(
            job_id=73, submit=1000, restart=0, state="FAILED",
            het_group_id="71", het_group="g1",
        )])])
        jobs = self.merge.jobs()
        self.assertEqual(len(jobs), 1)
        attempt = jobs[0]["attempts"][0]
        self.assertEqual(attempt["attempt_key"], "science-a|71|1000|0")
        components = [entry["label"] for entry in jobs[0]["components"]]
        self.assertEqual(sorted(components), ["g0", "g1"])
        self.assertEqual([entry["label"] for entry in jobs[0]["steps"]], ["batch"])
        self.assertEqual(len(jobs[0]["attempts"]), 1)

    def test_orphan_child_does_not_create_top_level_attempt(self):
        self.feed([observation(jobs=[job(
            job_id=72, array_job_id=71, array_task_id=1, state="COMPLETED",
        )])])
        self.assertEqual(self.merge.jobs(), [])
        self.assertEqual(self.merge.connection.execute(
            "SELECT count(*) FROM attempts").fetchone()[0], 0)
        self.assertEqual(self.merge.connection.execute(
            "SELECT count(*) FROM run_records").fetchone()[0], 1)

    def test_child_appearing_before_owner_in_same_response(self):
        self.feed([observation(jobs=[
            {"job_id": 73, "submit_time": 1000, "restart_cnt": 0, "state": ["FAILED"],
             "het_group_id": "71", "het_group": "g1"},
            job(state="FAILED"),
        ])])
        jobs = self.merge.jobs()
        self.assertEqual(len(jobs), 1)
        self.assertEqual([entry["label"] for entry in jobs[0]["components"]], ["g1"])

    def test_embedded_components_are_recorded_under_owner(self):
        self.feed([observation(jobs=[job(state="RUNNING", het=[
            {"het_group": "g0", "job_id": 74, "state": ["RUNNING"]},
            {"het_group": "g1", "job_id": 75, "state": ["PENDING"]},
        ])])])
        jobs = self.merge.jobs()
        self.assertEqual(len(jobs), 1)
        self.assertEqual(len(jobs[0]["attempts"]), 1)
        self.assertEqual([entry["label"] for entry in jobs[0]["components"]], ["g0", "g1"])


class AbsenceTests(MergeCase):
    def test_empty_response_never_creates_cancellation(self):
        self.feed([observation(source="controller", jobs=[job(state="RUNNING")])])
        attempt_key = self.merge.jobs()[0]["attempts"][0]["attempt_key"]
        self.feed([], source="controller")
        self.assertEqual(self.merge.jobs()[0]["attempts"][0]["sides"]["controller"]["state"], "RUNNING")
        actions = [event["action"] for event in self.merge.events(attempt_key)]
        self.assertNotIn("cancellation_recorded", actions)
        self.assertIn("empty_observation", [event["action"] for event in self.merge.events()])
        self.assertEqual(self.merge.jobs()[0]["attempts"][0]["sides"]["controller"]["state"], "RUNNING")

    def test_non_success_response_is_not_source_records(self):
        self.feed([observation(source="controller", jobs=[job(state="RUNNING")])])
        self.feed([], source="controller", status=404)
        jobs = self.merge.jobs()
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["attempts"][0]["sides"]["controller"]["state"], "RUNNING")
        failures = self.merge.connection.execute("SELECT * FROM ingest_failures").fetchall()
        self.assertEqual(len(failures), 1)

    def test_undecodable_body_is_isolated_from_raw_archive(self):
        outcome = self.feed([], raw=b"not json", content_type="application/octet-stream")
        self.assertTrue(outcome["skipped"])
        self.assertEqual(len(self.archive.captures()), 1)
        self.assertEqual(self.merge.jobs(), [])


class ResultViewTests(MergeCase):
    def test_mixed_children_and_attempts_in_one_response(self):
        self.feed([observation(jobs=[
            job(state="RUNNING", array_job_id=71, array_task_string="1-3"),
            job(job_id=72, array_job_id=71, array_task_id=1, state="COMPLETED"),
            {"job_id": 73, "submit_time": 1000, "restart_cnt": 0, "state": ["RUNNING"],
             "het_group_id": "71", "het_group": "g1"},
            job(state="PENDING", restart=1),
        ])])
        jobs = self.merge.jobs()
        self.assertEqual(len(jobs), 1)
        self.assertEqual([a["restart_cnt"] for a in jobs[0]["attempts"]], [0, 1])
        self.assertEqual(jobs[0]["array"]["materialized"], 1)
        self.assertEqual([c["label"] for c in jobs[0]["components"]], ["g1"])
        # 同一响应里不同尝试的状态互不串写。
        self.assertEqual(jobs[0]["attempts"][0]["sides"]["controller"]["state"], "RUNNING")
        self.assertEqual(jobs[0]["attempts"][1]["sides"]["controller"]["state"], "PENDING")

    def test_views_are_json_serialisable(self):
        self.feed([observation(source="controller", jobs=[job(
            state="RUNNING", array_job_id=71, array_task_string="1-4",
        )])])
        self.feed([observation(source="accounting", version=2, jobs=[job(
            state="FAILED", steps=[{"step_id": "batch", "exit_code": 1}],
        )])])
        for value in (self.merge.jobs(), self.merge.pending(), self.merge.resume()):
            json.dumps(value)
        self.assertEqual(self.merge.resume()["replayed"], 0)


class PersistenceTests(MergeCase):
    def test_reingest_of_same_capture_is_idempotent(self):
        capture_id = self.archive.append(
            "fixture", "/feed", 200, {"content-type": "application/json"},
            json.dumps({"items": [observation(jobs=[job()])]}).encode(),
        )
        first = self.merge.ingest_capture(capture_id)
        second = self.merge.ingest_capture(capture_id)
        self.assertFalse(first.get("skipped", False))
        self.assertTrue(second["skipped"])
        self.assertEqual(len(self.merge.jobs()[0]["attempts"]), 1)

    def test_state_survives_reopen_and_resume_finishes_pending(self):
        self.feed([observation(source="controller", jobs=[job(state="RUNNING")])])
        key = self.merge.jobs()[0]["attempts"][0]["attempt_key"]
        path = self.archive.path
        self.archive.close()
        self.archive = Archive(path)
        self.merge = Merge(self.archive)
        self.assertEqual(self.merge.attempt(key)["sides"]["controller"]["state"], "RUNNING")
        # 账务在重启之后才补齐。
        self.feed([observation(source="accounting", version=2, jobs=[job(state="COMPLETED")])])
        resumed = self.merge.resume()
        self.assertEqual(resumed["attempts"], 1)
        self.assertEqual([entry["attempt_key"] for entry in resumed["pending"]], [key])
        self.assertEqual(self.merge.jobs()[0]["attempts"][0]["sides"]["accounting"]["state"], "COMPLETED")
        self.assertFalse(self.merge.jobs()[0]["attempts"][0]["settled"])
        # 控制器终态补齐后才结项。
        self.feed([observation(source="controller", version=3, jobs=[job(state="COMPLETED")])])
        self.assertEqual(self.merge.resume()["pending"], [])
        self.assertTrue(self.merge.jobs()[0]["attempts"][0]["settled"])
        self.assertEqual(self.merge.attempt(key)["sides"]["controller"]["state"], "COMPLETED")

    def test_replay_reprocesses_only_unexplained_captures(self):
        first = self.feed([observation(jobs=[job()])])
        # 模拟进程在归档后、汇合前退出：删掉业务侧运行记录。
        self.merge.connection.execute("DELETE FROM evidence")
        self.merge.connection.execute("DELETE FROM attempt_members")
        self.merge.connection.execute("DELETE FROM attempt_details")
        self.merge.connection.execute("DELETE FROM attempt_sides")
        self.merge.connection.execute("DELETE FROM attempt_status")
        self.merge.connection.execute("DELETE FROM attempts")
        self.merge.connection.execute("DELETE FROM run_records")
        self.merge.connection.execute("DELETE FROM source_runs")
        self.assertFalse(first.get("skipped", False))
        self.assertEqual(self.merge.jobs(), [])
        replayed = self.merge.replay()
        self.assertEqual(len(replayed), 1)
        self.assertEqual(len(self.merge.jobs()), 1)
        self.assertEqual(self.merge.replay(), [])


class LiveTests(MergeCase):
    def test_live_polling_against_local_origin_keeps_raw_and_merged(self):
        with serve() as origin:
            client = Client(origin.base_url)
            result = merge_capture(self.merge, client, "local")
            self.assertEqual(result["status"], 200)
            self.assertEqual(result["merge"]["runs"], 3)
            jobs = self.merge.jobs()
            self.assertEqual(len(jobs), 1)
            self.assertEqual(len(jobs[0]["attempts"]), 2)
            self.assertEqual(self.merge.response(result["capture_id"])["status"], 200)
            self.assertEqual(len(unfinished(self.merge)), 2)

    def test_backfill_records_retention_gap_without_inventing_state(self):
        with serve() as origin:
            client = Client(origin.base_url)
            entries = backfill(self.merge, client, ["/observations/1", "/observations/2", "/not-found"],
                               "local")
            self.assertEqual([entry.get("status") for entry in entries], [200, 200, 404])
            self.assertEqual(entries[-1]["retention"], "expired")
            attempts = {entry["attempt_key"]: entry for entry in self.merge.jobs()[0]["attempts"]}
            self.assertNotIn("accounting", attempts["science-a|71|1000|0"]["sides"])
            # 404 不形成终态，也不形成取消事件。
            self.assertNotIn("cancellation_recorded", [event["action"] for event in self.merge.events()])

    def test_backfill_then_realtime_converges(self):
        with serve() as origin:
            client = Client(origin.base_url)
            backfill(self.merge, client, ["/observations/1", "/observations/3"], "local")
            attempt = self.merge.jobs()[0]["attempts"][0]
            self.assertEqual(attempt["sides"]["controller"]["state"], "RUNNING")
            self.assertEqual(attempt["sides"]["accounting"]["state"], "FAILED")
            self.assertEqual(attempt["outstanding"], ["controller"])
            # 实时轮询补齐控制器终态。
            with serve({"routes": {"/feed": {"json": {"items": [observation(
                source="controller", version=9, jobs=[job(state="FAILED")],
            )]}}}, "portal": {"csrf": "x"}, "source_records": [], "identity_fields": []}) as second:
                merge_capture(self.merge, Client(second.base_url), "local")
            attempt = self.merge.jobs()[0]["attempts"][0]
            self.assertTrue(attempt["settled"])
            self.assertEqual(self.merge.pending(), [])


class EvidenceTests(MergeCase):
    def test_evidence_links_business_view_to_raw_response(self):
        self.feed([observation(source="accounting", jobs=[job(
            state="FAILED", steps=[{"step_id": "batch", "exit_code": 1}],
        )])])
        key = self.merge.jobs()[0]["attempts"][0]["attempt_key"]
        evidence = self.merge.evidence(key)
        kinds = [entry["kind"] for entry in evidence]
        self.assertIn("state", kinds)
        self.assertIn("step", kinds)
        for entry in evidence:
            capture_id = entry["capture"]["capture_id"]
            self.assertIsNotNone(capture_id)
            raw = self.merge.response(capture_id)
            self.assertEqual(raw["status"], 200)
            self.assertIn("FAILED", raw["body"])

    def test_evidence_for_unknown_attempt_is_an_error(self):
        with self.assertRaises(KeyError):
            self.merge.evidence("missing|0|0|0")


if __name__ == "__main__":
    unittest.main()
