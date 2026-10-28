"""The capture flags: the redacted config copy, the tracing settings read from
the config, the tag, the trace match. Run: python3 -m unittest discover -s tests -v"""

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

TOOL = Path(__file__).resolve().parents[1] / "lz-ai-query.py"
spec = importlib.util.spec_from_file_location("lz_ai_query", TOOL)
tool = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = tool
spec.loader.exec_module(tool)

CONFIG = """MODEL_TYPE: anthropic
MODEL_NAME: claude-opus-5-5
API_KEY: sk-live-secret
TRACING_ENABLED: true
TRACING_SERVER_URL: http://phoenix.example.com/v1/traces
TRACING_PROJECT_NAME: lab-1
TRACING_API_KEY: ''
EXTRA_MCP_URLS: []
SOME_TOKEN: abc
EMPTY_KEY:
"""


class RedactionTests(unittest.TestCase):
    def test_secret_values_are_replaced_and_the_rest_kept(self):
        out = tool.redact_config_text(CONFIG)
        self.assertNotIn("sk-live-secret", out)
        self.assertIn("API_KEY: <redacted>\n", out)
        self.assertIn("TRACING_API_KEY: <redacted>\n", out)
        self.assertIn("SOME_TOKEN: <redacted>\n", out)
        self.assertIn("MODEL_NAME: claude-opus-5-5\n", out)
        self.assertIn("TRACING_SERVER_URL: http://phoenix.example.com/v1/traces\n", out)
        self.assertIn("EMPTY_KEY:\n", out)


class TracingSettingsTests(unittest.TestCase):
    def test_the_phoenix_base_and_project_come_from_the_config(self):
        self.assertEqual(tool.tracing_from_config(CONFIG),
                         ("http://phoenix.example.com", "lab-1"))

    def test_missing_settings_are_none(self):
        self.assertEqual(tool.tracing_from_config("MODEL_TYPE: x\n"), (None, None))


class TagTests(unittest.TestCase):
    def test_tag_pattern(self):
        for ok in ("llama-70b", "rtx5090.q4_k_m", "a"):
            self.assertTrue(tool.TAG_RE.match(ok), ok)
        for bad in ("", "-x", "a b", "a/b", "x" * 65):
            self.assertFalse(tool.TAG_RE.match(bad), bad)

    def test_results_land_under_the_tag(self):
        result = tool.QueryResult("Top hosts?")
        result.start_time = 1700000000.0
        with tempfile.TemporaryDirectory() as tmp:
            base = tool.result_base(result, tmp, "tag-1")
            self.assertEqual(base.parent, Path(tmp) / "tag-1")
            self.assertTrue(base.parent.is_dir())
            self.assertTrue(base.name.startswith("20231114T221320Z-top-hosts"))
            self.assertEqual(tool.trace_path_for(base).name, base.name + "-trace.json")


def fake_phoenix(roots, traces):
    def graphql(base, query, variables, verify_tls=True):
        if "getProjectByName" in query:
            return {"getProjectByName": {"id": "UHJvamVjdDox", "name": "lab-1"}}
        if "Roots" in query:
            return {"node": {"spans": {"edges": [{"node": r} for r in roots]}}}
        return {"getTraceByOtelId": traces[variables["traceId"]]}
    return graphql


def root(trace_id, start):
    return {"context": {"traceId": trace_id}, "startTime": start,
            "parentId": None, "name": "FunctionAgent.run"}


def trace_doc(trace_id, texts):
    spans = []
    for i, text in enumerate(texts):
        spans.append({
            "id": f"{trace_id}-{i}", "name": "FunctionAgent.setup_agent",
            "parentId": None, "context": {"traceId": trace_id, "spanId": f"s{i}"},
            "input": {"value": "{}"},
            "output": {"value": json.dumps({"messages": [{"content": text}]})},
        })
    return {"traceId": trace_id, "startTime": "2023-11-14T22:13:21+00:00",
            "endTime": "2023-11-14T22:13:30+00:00", "latencyMs": 9000,
            "numSpans": len(spans), "spans": {"edges": [{"node": s} for s in spans]}}


class TraceMatchTests(unittest.TestCase):
    def setUp(self):
        self.result = tool.QueryResult("Top 10 hosts\nwith errors?")
        self.result.start_time = 1700000000.0   # 2023-11-14T22:13:20Z
        self.result.end_time = 1700000020.0
        self.original = tool.phoenix_graphql

    def tearDown(self):
        tool.phoenix_graphql = self.original

    def test_the_trace_carrying_the_question_is_exported(self):
        roots = [root("t1", "2023-11-14T22:13:21+00:00"),
                 root("t2", "2023-11-14T22:13:25.500000+00:00")]
        traces = {"t1": trace_doc("t1", ["something else"]),
                  "t2": trace_doc("t2", ["Top 10 hosts\nwith errors?"])}
        tool.phoenix_graphql = fake_phoenix(roots, traces)
        status, ids, trace = tool.find_trace("http://p", "pid", self.result)
        self.assertEqual((status, ids), ("exported", ["t2"]))
        self.assertEqual(trace["trace"]["traceId"], "t2")
        self.assertEqual(len(trace["spans"]), 1)

    def test_a_root_outside_the_window_is_not_a_candidate(self):
        roots = [root("t1", "2023-11-14T22:14:30+00:00")]
        traces = {"t1": trace_doc("t1", ["Top 10 hosts with errors?"])}
        tool.phoenix_graphql = fake_phoenix(roots, traces)
        self.assertEqual(tool.find_trace("http://p", "pid", self.result),
                         ("missing", [], None))

    def test_two_traces_with_the_question_are_ambiguous(self):
        roots = [root("t1", "2023-11-14T22:13:21+00:00"),
                 root("t2", "2023-11-14T22:13:22+00:00")]
        traces = {t: trace_doc(t, ["Top 10 hosts\nwith errors?"]) for t in ("t1", "t2")}
        tool.phoenix_graphql = fake_phoenix(roots, traces)
        self.assertEqual(tool.find_trace("http://p", "pid", self.result),
                         ("ambiguous", ["t1", "t2"], None))

    def test_more_candidates_than_the_cap_is_ambiguous_without_an_export(self):
        roots = [root(f"t{i}", "2023-11-14T22:13:21+00:00") for i in range(6)]
        tool.phoenix_graphql = fake_phoenix(roots, {})
        status, ids, trace = tool.find_trace("http://p", "pid", self.result)
        self.assertEqual((status, len(ids), trace), ("ambiguous", 6, None))


if __name__ == "__main__":
    unittest.main()
