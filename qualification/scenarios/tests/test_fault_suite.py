"""The local suite must reject missing, stale or failed fault evidence."""

import importlib.util
from pathlib import Path
import tempfile
import time
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "fault_suite.py"


class FaultSuiteTest(unittest.TestCase):
    def setUp(self):
        self.assertTrue(SCRIPT.exists(), "Task 22 has no method-level fault evidence collector")
        spec = importlib.util.spec_from_file_location("fault_suite", SCRIPT)
        self.suite = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.suite)
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.report = self.root / "signaling-integration-tests/target/failsafe-reports/TEST-native.xml"
        self.report.parent.mkdir(parents=True)
        self.started = time.time() - 1
        self.matrix = {
            "matrixVersion": 2,
            "faults": [
                {"id": "commit", "scope": "LOCAL_NATIVE_POSTGRES", "localChecks": [
                    {"module": "signaling-integration-tests", "class": "test.NativeIT", "methods": ["commitSurvives"]}
                ]},
                {"id": "az-loss", "scope": "EXTERNAL_THREE_AZ", "requiredEvidence": ["physicalFenceReceipt"]},
            ],
        }
        self.write_report()

    def write_report(self, name="commitSurvives", child="", tests="1", failures="0", skipped="0"):
        self.report.write_text(
            f'<testsuite tests="{tests}" failures="{failures}" errors="0" skipped="{skipped}">'
            f'<testcase classname="test.NativeIT" name="{name}" time="0.1">{child}</testcase></testsuite>'
        )

    def collect(self):
        return self.suite.collect(self.root, self.matrix, self.started, time.time())

    def test_fresh_method_evidence_never_passes_external_drill(self):
        result = self.collect()
        self.assertEqual(result["status"], "LOCAL_SUITE_PASSED")
        self.assertTrue(result["testOnly"])
        self.assertEqual(result["productionQualification"], "NOT_QUALIFIED")
        self.assertEqual(result["faults"][0]["localStatus"], "PASS")
        self.assertEqual(result["faults"][1]["localStatus"], "NOT_APPLICABLE")
        self.assertEqual(result["faults"][1]["externalStatus"], "NOT_RUN")
        self.assertEqual(result["reports"][0]["sha256"], self.suite.sha256(self.report))

    def test_wrong_method_and_absent_mapping_cannot_pass(self):
        self.write_report(name="someOtherPassingTest")
        with self.assertRaises(ValueError):
            self.collect()
        self.write_report()
        del self.matrix["faults"][0]["localChecks"]
        with self.assertRaises(ValueError):
            self.collect()

    def test_failure_error_skip_and_zero_execution_fail_closed(self):
        for child, tests, failures, skipped in [
            ('<failure message="fenced invariant violated"/>', "1", "1", "0"),
            ('<error message="fixture unavailable"/>', "1", "0", "0"),
            ('<skipped/>', "1", "0", "1"),
            ("", "0", "0", "0"),
        ]:
            with self.subTest(child=child, tests=tests):
                self.write_report(child=child, tests=tests, failures=failures, skipped=skipped)
                with self.assertRaises(ValueError):
                    self.collect()

    def test_stale_report_and_invalid_counts_are_rejected(self):
        import os
        os.utime(self.report, (self.started - 30, self.started - 30))
        with self.assertRaises(ValueError):
            self.collect()
        for value in ("true", "-1", "1.0", "2"):
            self.write_report(tests=value)
            with self.assertRaises(ValueError):
                self.collect()

    def test_duplicate_case_duplicate_fault_and_path_escape_are_rejected(self):
        self.report.with_name("TEST-duplicate.xml").write_bytes(self.report.read_bytes())
        with self.assertRaises(ValueError):
            self.collect()
        self.report.with_name("TEST-duplicate.xml").unlink()
        self.matrix["faults"].append(dict(self.matrix["faults"][0]))
        with self.assertRaises(ValueError):
            self.collect()
        self.matrix["faults"].pop()
        self.matrix["faults"][0]["localChecks"][0]["module"] = "../outside"
        with self.assertRaises(ValueError):
            self.collect()

    def test_fault_receipt_must_be_fresh_local_and_match_the_scenario(self):
        import json
        import os
        fault = self.matrix["faults"][0]
        fault["localReceipts"] = [{"module": "signaling-integration-tests", "scenario": "commit"}]
        with self.assertRaises(ValueError):
            self.collect()
        receipt = self.root / "signaling-integration-tests/target/fault-receipts/commit.json"
        receipt.parent.mkdir(parents=True)
        body = {"testOnly": True, "scope": "LOCAL_TEST", "scenario": "commit", "observations": {"epoch": 2}}
        receipt.write_text(json.dumps(body))
        self.assertEqual(self.collect()["faults"][0]["receipts"][0]["sha256"], self.suite.sha256(receipt))
        for field, value in [("testOnly", False), ("scenario", "other"), ("scope", "EXTERNAL_THREE_AZ"), ("observations", {})]:
            invalid = {**body, field: value}
            receipt.write_text(json.dumps(invalid))
            with self.assertRaises(ValueError):
                self.collect()
        receipt.write_text(json.dumps(body))
        os.utime(receipt, (self.started - 30, self.started - 30))
        with self.assertRaises(ValueError):
            self.collect()

    def test_malformed_matrix_is_a_controlled_failure(self):
        import copy
        original = copy.deepcopy(self.matrix)
        for fault in [None, [], {"id": "invalid", "scope": "LOCAL_FABRICATED", "localChecks": []}]:
            self.matrix = {"matrixVersion": 2, "faults": [fault]}
            with self.assertRaises(ValueError):
                self.collect()
        for field, value in [("class", []), ("methods", [[]]), ("module", None)]:
            self.matrix = copy.deepcopy(original)
            self.matrix["faults"][0]["localChecks"][0][field] = value
            with self.assertRaises(ValueError):
                self.collect()
        self.matrix = original

    def test_parameterized_invocations_are_all_retained_under_the_required_method(self):
        self.report.write_text(
            '<testsuite tests="2" failures="0" errors="0" skipped="0">'
            '<testcase classname="test.NativeIT" name="commitSurvives(boolean)[1]"/>'
            '<testcase classname="test.NativeIT" name="commitSurvives(boolean)[2]"/>'
            '</testsuite>'
        )
        self.assertEqual(self.collect()["totals"]["tests"], 2)

    def test_jqwik_simple_classname_is_bound_to_the_original_qualified_suite_name(self):
        self.report.write_text(
            '<testsuite name="test.NativeIT" tests="1" failures="0" errors="0" skipped="0">'
            '<testcase classname="NativeIT" name="commitSurvives"/>'
            '</testsuite>'
        )
        self.assertEqual(self.collect()["faults"][0]["checks"][0]["class"], "test.NativeIT")
        self.report.write_text(self.report.read_text().replace('name="test.NativeIT"', 'name="other.UnrelatedIT"'))
        with self.assertRaises(ValueError):
            self.collect()


if __name__ == "__main__":
    unittest.main()
