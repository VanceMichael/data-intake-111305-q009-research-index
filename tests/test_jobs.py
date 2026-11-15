import json
import tempfile
import unittest
from pathlib import Path

from research_index.archive import Archive
from research_index.client import Client, collect
from research_index.jobs import JobLedger
from research_index.origin import load_scenario, serve


def feed_payload(envelopes):
    return json.dumps({"items": envelopes}, ensure_ascii=False).encode()


class LedgerTestBase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.archive = Archive(Path(self.temporary.name) / "raw.sqlite3")
        self.ledger = JobLedger(self.archive)

    def tearDown(self):
        self.archive.close()
        self.temporary.cleanup()

    def archive_feed(self, envelopes, source="fixture"):
        return self.archive.append(
            source, "/feed", 200, {"content-type": "application/json"}, feed_payload(envelopes))

    def merge_feed(self, envelopes):
        return self.ledger.merge_capture(self.archive_feed(envelopes))

    def merge_fixture_records(self, indexes):
        records = load_scenario()["source_records"]
        return self.merge_feed([records[index] for index in indexes])


class RequeueAttemptTests(LedgerTestBase):
    def test_requeued_job_is_one_logical_job_with_ordered_attempts(self):
        self.merge_fixture_records([0, 1, 2])
        jobs = self.ledger.logical_jobs()
        self.assertEqual(len(jobs), 1)
        job = jobs[0]
        self.assertEqual((job["cluster"], job["job_id"], job["submit_time"]),
                         ("science-a", 71, 1000))
        self.assertEqual([attempt["restart_cnt"] for attempt in job["attempts"]], [0, 1])
        first, second = job["attempts"]
        self.assertEqual(first["sources"]["controller"]["state"], ["RUNNING"])
        self.assertEqual(first["sources"]["accounting"]["state"], ["FAILED"])
        self.assertTrue(first["settled"])
        self.assertEqual(second["sources"]["controller"]["state"], ["PENDING"])
        self.assertNotIn("accounting", second["sources"])
        self.assertFalse(second["settled"])
        summary = self.ledger.summary()
        self.assertEqual(summary["logical_jobs"], 1)
        self.assertEqual(summary["attempts"], 2)
        self.assertEqual(summary["unsettled_attempts"], 1)

    def test_late_accounting_does_not_overwrite_controller_view(self):
        # 账务先到、控制器后到：两侧各自保留，不按到达时间覆盖。
        self.merge_fixture_records([2])
        self.merge_fixture_records([0, 1])
        job = self.ledger.logical_jobs()[0]
        first, second = job["attempts"]
        self.assertEqual(first["sources"]["controller"]["state"], ["RUNNING"])
        self.assertEqual(first["sources"]["accounting"]["state"], ["FAILED"])
        self.assertEqual(first["sources"]["accounting"]["observation"], 3)
        self.assertTrue(first["settled"])
        self.assertEqual(second["sources"]["controller"]["state"], ["PENDING"])
        self.assertNotIn("accounting", second["sources"])

    def test_same_source_orders_by_observation_not_arrival(self):
        self.merge_feed([{"cluster": "science-a", "source": "controller", "observation": 5,
                          "jobs": [{"job_id": 60, "submit_time": 900, "state": ["RUNNING"]}]}])
        self.merge_feed([{"cluster": "science-a", "source": "controller", "observation": 3,
                          "jobs": [{"job_id": 60, "submit_time": 900, "state": ["PENDING"]}]}])
        attempt = self.ledger.logical_jobs()[0]["attempts"][0]
        self.assertEqual(attempt["sources"]["controller"]["state"], ["RUNNING"])
        self.assertEqual(attempt["sources"]["controller"]["observation"], 5)
        detail = self.ledger.job_detail("science-a", 60)
        self.assertEqual(len(detail["attempts"][0]["evidence"]), 2)

    def test_steps_attach_to_the_attempt_not_to_jobs(self):
        self.merge_fixture_records([0, 1, 2])
        job = self.ledger.logical_jobs()[0]
        self.assertEqual(len(self.ledger.logical_jobs()), 1)
        steps = job["attempts"][0]["steps"]
        self.assertEqual([(step["step_id"], step["exit_code"]) for step in steps],
                         [("batch", 1)])
        self.assertEqual(job["attempts"][1]["steps"], [])


class ArrayAndComponentTests(LedgerTestBase):
    def test_array_placeholder_range_is_not_materialized(self):
        self.merge_fixture_records([0, 1, 2])
        jobs = self.ledger.logical_jobs()
        self.assertEqual(len(jobs), 1)
        job = jobs[0]
        self.assertEqual(job["role"], "array_placeholder")
        self.assertFalse(job["counts_as_task"])
        self.assertEqual(job["array"]["array_task_string"], "1-4")
        summary = self.ledger.summary()
        self.assertEqual(summary["counted_tasks"], 0)
        self.assertEqual(summary["array_placeholders"], 1)

    def test_materialized_array_member_counts_once_and_links_to_array(self):
        self.merge_feed([{
            "cluster": "science-a", "source": "controller", "observation": 10,
            "jobs": [
                {"job_id": 71, "submit_time": 1000, "state": ["PENDING"],
                 "array_job_id": 71, "array_task_string": "1-4"},
                {"job_id": 82, "submit_time": 1005, "state": ["RUNNING"],
                 "array_job_id": 71, "array_task_id": 2},
            ],
        }])
        jobs = {job["job_id"]: job for job in self.ledger.logical_jobs()}
        self.assertEqual(jobs[82]["role"], "array_member")
        self.assertTrue(jobs[82]["counts_as_task"])
        detail = self.ledger.job_detail("science-a", 71)
        self.assertEqual([member["job_id"] for member in detail["members"]], [82])
        self.assertEqual(detail["members"][0]["array_task_id"], 2)
        self.assertEqual(self.ledger.summary()["counted_tasks"], 1)

    def test_het_component_is_not_a_separate_top_level_job(self):
        self.merge_feed([{
            "cluster": "science-a", "source": "controller", "observation": 11,
            "jobs": [
                {"job_id": 90, "submit_time": 2000, "state": ["RUNNING"],
                 "het_job_id": 90, "het_job_offset": 0},
                {"job_id": 91, "submit_time": 2000, "state": ["RUNNING"],
                 "het_job_id": 90, "het_job_offset": 1},
            ],
        }])
        jobs = {job["job_id"]: job for job in self.ledger.logical_jobs()}
        self.assertEqual(jobs[90]["role"], "het_leader")
        self.assertEqual(jobs[91]["role"], "het_component")
        self.assertFalse(jobs[91]["counts_as_task"])
        detail = self.ledger.job_detail("science-a", 90)
        self.assertEqual([component["job_id"] for component in detail["components"]], [91])
        self.assertEqual(self.ledger.summary()["counted_tasks"], 1)
        self.assertEqual(self.ledger.summary()["het_components"], 1)


class IdentityTests(LedgerTestBase):
    def test_job_id_is_not_a_global_key(self):
        self.merge_feed([
            {"cluster": "science-a", "source": "controller", "observation": 1,
             "jobs": [{"job_id": 71, "submit_time": 1000, "state": ["RUNNING"]}]},
            {"cluster": "science-b", "source": "controller", "observation": 2,
             "jobs": [{"job_id": 71, "submit_time": 1000, "state": ["RUNNING"]}]},
            {"cluster": "science-a", "source": "controller", "observation": 3,
             "jobs": [{"job_id": 71, "submit_time": 5000, "state": ["PENDING"]}]},
        ])
        jobs = self.ledger.logical_jobs()
        self.assertEqual(len(jobs), 3)
        self.assertEqual(len(self.ledger.logical_jobs("science-b")), 1)

    def test_detail_requires_submit_time_when_job_id_is_reused(self):
        self.merge_feed([
            {"cluster": "science-a", "source": "controller", "observation": 1,
             "jobs": [{"job_id": 71, "submit_time": 1000, "state": ["RUNNING"]}]},
            {"cluster": "science-a", "source": "controller", "observation": 2,
             "jobs": [{"job_id": 71, "submit_time": 5000, "state": ["PENDING"]}]},
        ])
        with self.assertRaisesRegex(ValueError, "submit_time"):
            self.ledger.job_detail("science-a", 71)
        detail = self.ledger.job_detail("science-a", 71, submit_time=5000)
        self.assertEqual(detail["submit_time"], 5000)
        with self.assertRaises(KeyError):
            self.ledger.job_detail("science-a", 999)


class AbsenceTests(LedgerTestBase):
    def test_missing_record_never_fabricates_cancellation(self):
        self.merge_fixture_records([0])
        before = self.ledger.logical_jobs()
        self.merge_feed([{"cluster": "science-a", "source": "controller",
                          "observation": 9, "jobs": []}])
        after = self.ledger.logical_jobs()
        self.assertEqual(before, after)
        states = [source["state"] for job in after for attempt in job["attempts"]
                  for source in attempt["sources"].values()]
        self.assertNotIn(["CANCELLED"], states)
        self.assertEqual(len(self.ledger.outstanding()), 1)


class OutstandingTests(LedgerTestBase):
    def test_outstanding_survives_reopen_and_late_accounting_settles(self):
        self.merge_fixture_records([0, 1])
        outstanding = self.ledger.outstanding()
        self.assertEqual({(item["job_id"], item["restart_cnt"]) for item in outstanding},
                         {(71, 0), (71, 1)})
        self.assertTrue(next(item for item in outstanding if item["restart_cnt"] == 0)["ran"])
        self.assertFalse(next(item for item in outstanding if item["restart_cnt"] == 1)["ran"])
        # 模拟进程重启：未结项仍在，补扫可继续。
        self.archive.close()
        self.archive = Archive(self.archive.path)
        self.ledger = JobLedger(self.archive)
        self.assertEqual(len(self.ledger.outstanding()), 2)
        self.merge_fixture_records([2])
        outstanding = self.ledger.outstanding()
        self.assertEqual([(item["job_id"], item["restart_cnt"]) for item in outstanding],
                         [(71, 1)])
        self.assertEqual(outstanding[0]["controller_state"], ["PENDING"])
        self.assertIsNone(outstanding[0]["accounting_state"])
        self.assertTrue(self.ledger.logical_jobs()[0]["attempts"][0]["settled"])


class MergeRobustnessTests(LedgerTestBase):
    def test_remerge_is_idempotent_and_duplicate_observations_collapse(self):
        records = load_scenario()["source_records"]
        capture_id = self.archive_feed(records)
        first = self.ledger.merge_capture(capture_id)
        second = self.ledger.merge_capture(capture_id)
        self.assertFalse(first["skipped"])
        self.assertTrue(second["skipped"])
        # 同一观察经另一捕获再次到达（如 /feed 与 /observations/1 重叠）。
        self.merge_feed([records[0]])
        count = self.ledger.connection.execute("SELECT COUNT(*) FROM evidence").fetchone()[0]
        self.assertEqual(count, 3)
        inputs = self.ledger.connection.execute("SELECT COUNT(*) FROM merge_inputs").fetchone()[0]
        self.assertEqual(inputs, 2)
        self.assertEqual(self.ledger.summary()["attempts"], 2)

    def test_undecodable_capture_is_reported_and_others_still_merge(self):
        bad = self.archive.append("fixture", "/plain", 200,
                                  {"content-type": "text/plain"}, b"not records")
        good = self.archive_feed(load_scenario()["source_records"])
        results = {result["capture_id"]: result for result in self.ledger.merge_pending()}
        self.assertIn("error", results[bad])
        self.assertNotIn("error", results[good])
        self.assertEqual(len(self.ledger.logical_jobs()), 1)
        # 未成功汇合的捕获未标记，可在修正后重试。
        pending = [result["capture_id"] for result in self.ledger.merge_pending()]
        self.assertEqual(pending, [bad])

    def test_non_success_capture_records_no_jobs(self):
        capture_id = self.archive.append(
            "fixture", "/rate-limited", 429, {"retry-after": "2"}, b'{"error": "quota"}')
        result = self.ledger.merge_capture(capture_id)
        self.assertEqual(result["records_seen"], 0)
        self.assertEqual(self.ledger.logical_jobs(), [])

    def test_invalid_records_are_rejected_atomically(self):
        with self.assertRaisesRegex(ValueError, "job_id"):
            self.merge_feed([{"cluster": "science-a", "source": "controller", "observation": 1,
                              "jobs": [{"submit_time": 1}]}])
        self.assertEqual(self.ledger.logical_jobs(), [])
        count = self.ledger.connection.execute("SELECT COUNT(*) FROM merge_inputs").fetchone()[0]
        self.assertEqual(count, 0)
        with self.assertRaisesRegex(ValueError, "cluster"):
            self.merge_feed([{"source": "controller", "jobs": []}])
        with self.assertRaisesRegex(ValueError, "restart_cnt"):
            self.merge_feed([{"cluster": "science-a", "source": "controller", "observation": 1,
                              "jobs": [{"job_id": 5, "restart_cnt": -1}]}])


class DrillDownTests(LedgerTestBase):
    def test_detail_evidence_drills_down_to_source_response(self):
        records = load_scenario()["source_records"]
        capture_id = self.archive_feed(records)
        self.ledger.merge_capture(capture_id)
        detail = self.ledger.job_detail("science-a", 71)
        evidence = [item for attempt in detail["attempts"] for item in attempt["evidence"]]
        self.assertEqual({item["source"] for item in evidence}, {"controller", "accounting"})
        self.assertTrue(all(item["capture_id"] == capture_id for item in evidence))
        accounting = next(item for item in evidence if item["source"] == "accounting")
        self.assertEqual(accounting["detail"]["steps"][0]["exit_code"], 1)
        body = json.loads(self.archive.body(capture_id))
        self.assertEqual(len(body["items"]), 3)


class HttpMergeTests(LedgerTestBase):
    def test_capture_then_reconcile_over_http(self):
        with serve(load_scenario()) as origin:
            client = Client(origin.base_url)
            for path in ("/observations/1", "/observations/2"):
                collect(client, self.archive, "fixture", path)
            self.assertEqual(len(self.ledger.merge_pending()), 2)
            self.assertEqual(len(self.ledger.outstanding()), 2)
            collect(client, self.archive, "fixture", "/observations/3")
            self.assertEqual(len(self.ledger.merge_pending()), 1)
            jobs = self.ledger.logical_jobs()
            self.assertEqual(len(jobs), 1)
            self.assertTrue(jobs[0]["attempts"][0]["settled"])
            self.assertEqual(len(self.ledger.outstanding()), 1)
            detail = self.ledger.job_detail("science-a", 71)
            capture_ids = {item["capture_id"] for attempt in detail["attempts"]
                           for item in attempt["evidence"]}
            self.assertEqual(capture_ids, {1, 2, 3})
            for capture_id in capture_ids:
                self.assertTrue(self.archive.body(capture_id))


if __name__ == "__main__":
    unittest.main()
