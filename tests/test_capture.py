"""The capture flags: the redacted config copy, the tracing settings read from
the config, the tag, the trace match. Run: python3 -m unittest discover -s tests -v"""

import asyncio
import importlib.util
import json
import sys
import tempfile
import threading
import types
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

    def test_quoted_keys_and_block_scalars_are_redacted_whole(self):
        config = (
            "API_KEY: |-\n"
            "  sk-block-secret\n"
            "\n"
            "  second-line-of-secret\n"
            "MODEL_NAME: claude-opus-5-5\n"
            "\"SOME_TOKEN\": quoted-key-secret\n"
            "'OTHER_KEY': 'single-quoted-secret'\n"
            "FOLDED_TOKEN: >\n"
            "  folded secret\n"
            "TRACING_PROJECT_NAME: lab-1\n"
        )
        out = tool.redact_config_text(config)
        for secret in ("sk-block-secret", "second-line-of-secret", "quoted-key-secret",
                       "single-quoted-secret", "folded secret"):
            self.assertNotIn(secret, out)
        self.assertIn("API_KEY: <redacted>\n", out)
        self.assertIn("\"SOME_TOKEN\": <redacted>\n", out)
        self.assertIn("'OTHER_KEY': <redacted>\n", out)
        self.assertIn("FOLDED_TOKEN: <redacted>\n", out)
        self.assertIn("MODEL_NAME: claude-opus-5-5\n", out)
        self.assertIn("TRACING_PROJECT_NAME: lab-1\n", out)

    def test_a_value_on_the_lines_below_the_key_is_redacted_whole(self):
        config = (
            "API_KEY:\n"
            "  sk-next-line-secret\n"
            "SOME_TOKEN: \"first line\n"
            "  second-line-of-quoted-secret\"\n"
            "OTHER_KEY: plain first\n"
            "  plain-continuation-secret\n"
            "EMPTY_KEY:\n"
            "MODEL_NAME: claude-opus-5-5\n"
        )
        out = tool.redact_config_text(config)
        for secret in ("sk-next-line-secret", "second-line-of-quoted-secret",
                       "plain-continuation-secret", "first line", "plain first"):
            self.assertNotIn(secret, out)
        self.assertIn("API_KEY: <redacted>\n", out)
        self.assertIn("SOME_TOKEN: <redacted>\n", out)
        self.assertIn("OTHER_KEY: <redacted>\n", out)
        self.assertIn("EMPTY_KEY:\n", out)
        self.assertIn("MODEL_NAME: claude-opus-5-5\n", out)


class TracingSettingsTests(unittest.TestCase):
    def test_the_phoenix_base_and_project_come_from_the_config(self):
        self.assertEqual(tool.tracing_from_config(CONFIG),
                         ("http://phoenix.example.com", "lab-1"))

    def test_missing_settings_are_none(self):
        self.assertEqual(tool.tracing_from_config("MODEL_TYPE: x\n"), (None, None))

    def test_a_trailing_comment_is_not_part_of_the_value(self):
        config = (
            "TRACING_SERVER_URL: 'http://phoenix.example.com/v1/traces' # the lab\n"
            "TRACING_PROJECT_NAME: lab-1 # certification run\n"
        )
        self.assertEqual(tool.tracing_from_config(config),
                         ("http://phoenix.example.com", "lab-1"))
        quoted = "TRACING_PROJECT_NAME: 'lab #1' # note\n"
        self.assertEqual(tool.tracing_from_config(quoted)[1], "lab #1")


class DeployTests(unittest.TestCase):
    def test_an_ipv6_host_keeps_its_brackets_for_scp(self):
        scp, ssh = tool.deploy_steps("https://[2001:db8::1]", "ai.yaml")
        self.assertEqual(scp[-1], "root@[2001:db8::1]:" + tool.REMOTE_AI_YAML)
        self.assertEqual(ssh[1], "root@2001:db8::1")
        scp, ssh = tool.deploy_steps("http://lz.example.com", "ai.yaml")
        self.assertEqual(scp[-1], "root@lz.example.com:" + tool.REMOTE_AI_YAML)


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


def fake_phoenix(roots, traces, project_absent_calls=0, roots_empty_calls=0):
    """Phoenix as a function of the call count: the project is absent for the
    first `project_absent_calls` lookups and the roots listing empty for the
    first `roots_empty_calls`, the way a trace that has not been ingested looks."""
    calls = {"project": 0, "roots": 0}

    def graphql(base, query, variables, verify_tls=True):
        if "getProjectByName" in query:
            calls["project"] += 1
            if calls["project"] <= project_absent_calls:
                return {"getProjectByName": None}
            return {"getProjectByName": {"id": "UHJvamVjdDox", "name": "lab-1"}}
        if "Roots" in query:
            calls["roots"] += 1
            listed = [] if calls["roots"] <= roots_empty_calls else roots
            return {"node": {"spans": {"edges": [{"node": r} for r in listed]}}}
        return {"getTraceByOtelId": traces[variables["traceId"]]}
    graphql.calls = calls
    return graphql


class FakeClock:
    """A clock that only moves when the code under test sleeps."""

    def __init__(self):
        self.now = 1000.0
        self.sleeps = []

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds

    def clock(self):
        return self.now


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
        self.assertEqual(tool.find_trace("http://p", "pid", self.result, wait_s=0),
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

    def test_a_longer_question_containing_this_one_is_not_a_match(self):
        self.result = tool.QueryResult("Show errors")
        self.result.start_time = 1700000000.0
        self.result.end_time = 1700000020.0
        roots = [root("t1", "2023-11-14T22:13:21+00:00")]
        traces = {"t1": trace_doc("t1", ["Show errors from yesterday"])}
        tool.phoenix_graphql = fake_phoenix(roots, traces)
        self.assertEqual(tool.find_trace("http://p", "lab-1", self.result, wait_s=0),
                         ("missing", [], None))

    def test_the_lookup_waits_for_ingestion_then_exports(self):
        roots = [root("t1", "2023-11-14T22:13:21+00:00")]
        traces = {"t1": trace_doc("t1", ["Top 10 hosts\nwith errors?"])}
        tool.phoenix_graphql = fake_phoenix(roots, traces, roots_empty_calls=2)
        fake = FakeClock()
        status, ids, trace = tool.find_trace(
            "http://p", "lab-1", self.result, wait_s=60, sleep=fake.sleep, clock=fake.clock)
        self.assertEqual((status, ids), ("exported", ["t1"]))
        self.assertEqual(fake.sleeps, [tool.TRACE_POLL_S, tool.TRACE_POLL_S])

    def test_the_wait_is_bounded_and_zero_means_one_lookup(self):
        tool.phoenix_graphql = fake_phoenix([], {}, roots_empty_calls=99)
        fake = FakeClock()
        status, ids, trace = tool.find_trace(
            "http://p", "lab-1", self.result, wait_s=12, sleep=fake.sleep, clock=fake.clock)
        self.assertEqual((status, ids, trace), ("missing", [], None))
        self.assertEqual(sum(fake.sleeps), 12)
        fake = FakeClock()
        tool.phoenix_graphql = fake_phoenix([], {}, roots_empty_calls=99)
        tool.find_trace("http://p", "lab-1", self.result, wait_s=0,
                        sleep=fake.sleep, clock=fake.clock)
        self.assertEqual(fake.sleeps, [])

    def test_a_project_that_appears_with_its_first_trace_is_resolved_then(self):
        roots = [root("t1", "2023-11-14T22:13:21+00:00")]
        traces = {"t1": trace_doc("t1", ["Top 10 hosts\nwith errors?"])}
        graphql = fake_phoenix(roots, traces, project_absent_calls=1)
        tool.phoenix_graphql = graphql
        fake = FakeClock()
        status, ids, trace = tool.find_trace(
            "http://p", "lab-1", self.result, wait_s=60, sleep=fake.sleep, clock=fake.clock)
        self.assertEqual((status, ids), ("exported", ["t1"]))
        self.assertEqual(graphql.calls["project"], 2)

    def test_a_phoenix_failure_is_a_trace_error_not_an_exit(self):
        def failing(base, query, variables, verify_tls=True):
            raise tool.TraceError("cannot reach Phoenix at http://p/graphql: refused")
        tool.phoenix_graphql = failing
        with self.assertRaises(tool.TraceError):
            tool.find_trace("http://p", "lab-1", self.result, wait_s=0)

    def test_an_unreachable_phoenix_raises_rather_than_exits(self):
        original_post = tool.requests.post

        def refused(*args, **kwargs):
            raise tool.requests.exceptions.ConnectionError("refused")
        tool.requests.post = refused
        try:
            with self.assertRaises(tool.TraceError):
                tool.phoenix_graphql("http://p", "{ x }", {})
        finally:
            tool.requests.post = original_post


class EmitTests(unittest.TestCase):
    """A Phoenix failure after a captured answer is recorded on the question and
    the answer is still saved; the question fails (exit 2), the run goes on."""

    def setUp(self):
        self.original_find = tool.find_trace
        self.original_print = tool.print_answer
        tool.print_answer = lambda *a, **k: None

    def tearDown(self):
        tool.find_trace = self.original_find
        tool.print_answer = self.original_print

    def test_a_trace_error_is_recorded_and_the_answer_persisted(self):
        def failing(*args, **kwargs):
            raise tool.TraceError("Phoenix http://p/graphql answered HTTP 502: bad gateway")
        tool.find_trace = failing
        result = tool.QueryResult("Top hosts?")
        result.start_time = 1700000000.0
        result.end_time = 1700000020.0
        result.answer = "host-1 leads"
        with tempfile.TemporaryDirectory() as tmp:
            args = types.SimpleNamespace(
                export_trace=True, save_dir=tmp, tag=None, json=False, raw=False,
                keep_charts=False, insecure=False, phoenix_base="http://p",
                phoenix_project="lab-1", trace_wait=0)
            asyncio.run(tool._emit(result, args, lambda *a, **k: None, multiple=True))
            saved = list(Path(tmp).glob("*.json"))
            self.assertEqual(len(saved), 1)
            doc = json.loads(saved[0].read_text(encoding="utf-8"))
        self.assertEqual(result.trace["status"], "error")
        self.assertIn("HTTP 502", result.trace["error"])
        self.assertFalse(result.ok)
        self.assertEqual(doc["trace"]["status"], "error")
        self.assertEqual(doc["answer"], "host-1 leads")

    def test_the_lookup_runs_off_the_event_loop_thread(self):
        """With --same-conversation the socket stays open across questions; a
        lookup that blocks the loop (HTTP calls, the ingestion wait) would let
        the server's heartbeat close it, so the lookup runs in a worker thread."""
        seen = {}

        def recording(*args, **kwargs):
            seen["thread"] = threading.current_thread()
            return "missing", [], None
        tool.find_trace = recording
        result = tool.QueryResult("Top hosts?")
        result.start_time = 1700000000.0
        result.end_time = 1700000020.0
        result.answer = "host-1 leads"
        self.assertTrue(asyncio.iscoroutinefunction(tool._emit))
        with tempfile.TemporaryDirectory() as tmp:
            args = types.SimpleNamespace(
                export_trace=True, save_dir=tmp, tag=None, json=False, raw=False,
                keep_charts=False, insecure=False, phoenix_base="http://p",
                phoenix_project="lab-1", trace_wait=0)
            asyncio.run(tool._emit(result, args, lambda *a, **k: None, multiple=True))
        self.assertIsNot(seen["thread"], threading.main_thread())
        self.assertEqual(result.trace["status"], "missing")


if __name__ == "__main__":
    unittest.main()
