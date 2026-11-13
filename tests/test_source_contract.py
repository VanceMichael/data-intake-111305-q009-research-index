import hashlib
import json
import unittest

from research_index.client import Client
from research_index.decoding import parse_page, records as decode_records
from research_index.origin import load_scenario, serve


class SourceContractTests(unittest.TestCase):
    def test_domain_observations_preserve_source_distinctions(self):
        scenario = load_scenario()
        records = scenario["source_records"]
        with serve(scenario) as origin:
            client = Client(origin.base_url)
            old = records[0]["jobs"][0]
            current = records[1]["jobs"][0]
            accounting = records[2]["jobs"][0]
            self.assertEqual(old["job_id"], current["job_id"])
            self.assertNotEqual(old["restart_cnt"], current["restart_cnt"])
            self.assertEqual(accounting["restart_cnt"], old["restart_cnt"])
            self.assertIn("array_task_string", old)

    def test_every_record_has_the_declared_source_identity(self):
        scenario = load_scenario()
        for record in scenario["source_records"]:
            for field in scenario["identity_fields"]:
                self.assertIn(field, record)
                self.assertNotEqual(str(record[field]).strip(), "")


if __name__ == "__main__":
    unittest.main()
