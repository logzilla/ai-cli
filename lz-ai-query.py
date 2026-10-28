#!/usr/bin/env python3
"""Query the LogZilla AI assistant from the command line.

Sends a natural-language question to a LogZilla server's AI assistant over
its WebSocket chat API, streams back the progress (thinking, tool calls,
queries), and prints the final answer.

Quick start:
    pip install -r requirements.txt
    cp .env.example .env        # then edit .env with your host + API token
    ./lz-ai-query.py "How many events did we receive in the last hour?"

See README.md for full usage.
"""

import argparse
import asyncio
import json
import os
import re
import ssl
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
import websockets

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_TIMEOUT = 600
CONNECT_TIMEOUT = 30
MAX_WS_MESSAGE = 10 * 1024 * 1024  # 10 MB

# --- capture (the flags LogZilla's certification harness runs this tool with)
REMOTE_AI_YAML = "/etc/logzilla/settings/ai.yaml"
RELOAD_WAIT_DEFAULT = 30          # seconds the server takes to apply reloaded AI settings
TRACE_WINDOW_SLACK_S = 5          # a trace root may start this much before/after the question
TRACE_MAX_CANDIDATES = 5          # more roots in one question's window is ambiguous, not a guess
TRACE_MAX_SPANS = 500
TAG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
# a YAML line whose key names a secret: the value is replaced in the saved copy
SECRET_SETTING_RE = re.compile(r"^(\s*[A-Za-z0-9_]*(?:_KEY|_TOKEN|API_KEY)\s*:\s*)(?=\S).*$")


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

def load_dotenv_files():
    """Load .env from the current directory and next to this script.

    Existing environment variables always win, so `LZ_HOST=... ./lz-ai-query.py`
    overrides the file. Uses python-dotenv when available and falls back to a
    minimal parser so the script still works without it.
    """
    candidates = [Path.cwd() / ".env", SCRIPT_DIR / ".env"]
    try:
        from dotenv import load_dotenv
    except ImportError:
        load_dotenv = None

    for path in candidates:
        if not path.is_file():
            continue
        if load_dotenv:
            load_dotenv(path, override=False)
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            parsed = parse_env_line(line)
            if not parsed:
                continue
            key, value = parsed
            if key not in os.environ:
                os.environ[key] = value


def parse_env_line(line):
    """Parse one .env line into (key, value), or None if it is not a setting.

    Covers the conventions that actually show up in hand-written .env files:
    an optional `export ` prefix, single/double quoted values, and trailing
    `# comments`. Without these, `export LZ_HOST=...` would be stored under the
    key "export LZ_HOST" and silently ignored, and a trailing comment would end
    up inside the value.
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    # Optional `export ` prefix, as used when a .env is also `source`d.
    if line.startswith(("export ", "export\t")):
        line = line[len("export"):].lstrip()

    key, sep, value = line.partition("=")
    if not sep:
        return None
    key = key.strip()
    if not key:
        return None
    value = value.strip()

    if value[:1] in ("'", '"'):
        quote = value[0]
        end = value.find(quote, 1)
        # Quoted: the value is what is inside the quotes; anything after the
        # closing quote is a comment.
        return key, (value[1:end] if end != -1 else value[1:])

    if value.startswith("#"):
        return key, ""

    # Unquoted: only treat '#' as a comment when whitespace precedes it, so a
    # '#' inside a token or password stays part of the value.
    for i, char in enumerate(value):
        if char == "#" and value[i - 1] in " \t":
            value = value[:i]
            break
    return key, value.strip()


def _from_env(*names):
    """Return the first non-empty value among these environment variables."""
    for name in names:
        value = os.environ.get(name)
        if value and value.strip():
            return value.strip()
    return None


def resolve_config(args):
    """Return (host, token) from CLI flags or the environment.

    Both this tool's names (LZ_HOST / LZ_API_TOKEN) and the names used in
    LogZilla's own documentation (LOGZILLA_HOST / LOGZILLA_API_KEY) are
    accepted.
    """
    load_dotenv_files()

    host = args.host or _from_env("LZ_HOST", "LOGZILLA_HOST")
    token = args.token or _from_env("LZ_API_TOKEN", "LOGZILLA_API_KEY")

    missing = []
    if not host:
        missing.append("LZ_HOST (or LOGZILLA_HOST)")
    if not token:
        missing.append("LZ_API_TOKEN (or LOGZILLA_API_KEY)")
    if missing:
        die(
            f"missing required setting(s): {', '.join(missing)}\n"
            "Set them in a .env file (see .env.example), export them as "
            "environment variables, or pass --host / --token.\n"
            "Create a token on the LogZilla server with: "
            "sudo logzilla authtoken create"
        )

    token = token.strip()
    # Ingest-only tokens are valid for /incoming/* but rejected by /api/*,
    # which is what the AI assistant lives behind. Catch that up front rather
    # than surfacing it as a confusing auth failure.
    if token.startswith("ingest-"):
        die(
            "this is an ingest-only token, which LogZilla accepts only for "
            "event ingestion under /incoming and rejects for the API.\n"
            "Create a full user token instead: sudo logzilla authtoken create"
        )

    host = host.strip().rstrip("/")
    if re.search(r"\s", host):
        die(f"LZ_HOST contains whitespace: {host!r}")
    if "://" not in host:
        host = f"http://{host}"
    # LogZilla's own config examples use base_url = http://host/api, so accept
    # a pasted /api suffix instead of building //api/api/... from it.
    if host.endswith("/api"):
        host = host[: -len("/api")]

    parsed = urlparse(host)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        die(f"LZ_HOST is not a valid URL: {host}")

    return host, token


def die(message, code=1):
    print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(code)


def force_utf8_streams():
    """Make stdio UTF-8 regardless of locale.

    Answers routinely contain non-ASCII, and this script prints non-ASCII of
    its own, so printing under a C/POSIX or cp1252 locale would die with
    UnicodeEncodeError instead of showing the answer. Best effort: streams
    that cannot be reconfigured (already replaced or detached) are left alone.
    """
    for stream in (sys.stdin, sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, OSError, ValueError):
            pass


# --------------------------------------------------------------------------
# Result container
# --------------------------------------------------------------------------

class QueryResult:
    """Everything the server told us about one question."""

    def __init__(self, query):
        self.query = query
        self.conv_id = None
        self.start_time = None
        self.end_time = None
        self.answer = ""
        self.tool_calls = []
        self.tool_results = []
        self.errors = []
        self.stop_reason = None
        self.messages = []  # raw WebSocket messages, for debugging
        self.trace = None   # {'status', 'trace_ids', 'path'} when --export-trace ran

    @property
    def duration_s(self):
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0

    @property
    def duration_str(self):
        seconds = self.duration_s
        if seconds >= 60:
            return f"{seconds / 60:.1f} min ({seconds:.1f}s)"
        return f"{seconds:.1f}s"

    @property
    def ok(self):
        return bool(self.answer) and not self.errors

    def to_dict(self, include_raw=False):
        data = {
            "query": self.query,
            "answer": self.answer,
            "conversation_id": self.conv_id,
            "asked_at": (
                datetime.fromtimestamp(self.start_time, tz=timezone.utc)
                .isoformat().replace("+00:00", "Z")
                if self.start_time else None
            ),
            "duration_s": round(self.duration_s, 2),
            "tool_calls": self.tool_calls,
            "tool_results": self.tool_results,
            "errors": self.errors,
            "stop_reason": self.stop_reason,
        }
        if self.trace is not None:
            data["trace"] = self.trace
        if include_raw:
            data["raw_messages"] = self.messages
        return data


# --------------------------------------------------------------------------
# LogZilla AI client
# --------------------------------------------------------------------------

class LogZillaAI:
    """Minimal client for the LogZilla AI chat API."""

    def __init__(self, host, token, verify_tls=True, timeout=DEFAULT_TIMEOUT,
                 reporter=None):
        self.host = host
        self.token = token
        self.verify_tls = verify_tls
        self.timeout = timeout
        self.report = reporter or (lambda *_a, **_k: None)

    # -- REST ---------------------------------------------------------------

    @property
    def _headers(self):
        return {"Authorization": f"token {self.token}"}

    def create_conversation(self):
        """Create a fresh AI conversation and return its id."""
        url = f"{self.host}/api/ai-conversations"
        try:
            resp = requests.post(
                url, headers=self._headers, timeout=CONNECT_TIMEOUT,
                verify=self.verify_tls,
            )
        except requests.exceptions.SSLError as exc:
            die(f"TLS verification failed for {url}: {exc}\n"
                "If this server uses a self-signed certificate, re-run with "
                "--insecure.")
        except requests.exceptions.RequestException as exc:
            die(f"cannot reach LogZilla at {url}: {exc}")

        if resp.status_code in (401, 403):
            die(f"authentication failed ({resp.status_code}). Check your API "
                "token.\nCreate a fresh full user token on the LogZilla "
                "server with: sudo logzilla authtoken create\n"
                "(List existing tokens with: logzilla authtoken list)")
        if resp.status_code == 404:
            die(f"{url} returned 404. LogZilla's AI is probably not enabled "
                "on this server.\nEnable it in Settings -> System Settings -> "
                "AI Copilot (requires LogZilla v6.36 or newer).")
        if resp.status_code not in (200, 201):
            die(f"failed to create conversation: HTTP {resp.status_code} "
                f"{resp.text[:400]}")

        try:
            conv_id = resp.json()["id"]
        except (ValueError, KeyError, TypeError):
            die(f"unexpected response creating conversation: {resp.text[:400]}")

        self.report(f"conversation {conv_id} created", level="detail")
        return conv_id

    # -- WebSocket ----------------------------------------------------------

    def _ws_url(self, conv_id):
        """Build the chat WebSocket URL from the configured base URL.

        Uses netloc verbatim rather than hostname+port so that any explicit
        port, IPv6 literal (which needs its brackets), or userinfo survives,
        and keeps any base path so a LogZilla reverse-proxied under a subpath
        (https://host/logzilla) reaches the same place REST does.
        """
        parsed = urlparse(self.host)
        scheme = "wss" if parsed.scheme == "https" else "ws"
        base_path = parsed.path.rstrip("/")
        return f"{scheme}://{parsed.netloc}{base_path}/ws/ai-chat/{conv_id}"

    def _ssl_context(self, ws_url):
        if not ws_url.startswith("wss://"):
            return None
        context = ssl.create_default_context()
        if not self.verify_tls:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        return context

    @asynccontextmanager
    async def session(self, conv_id):
        """Open a chat session; yields an object with an async ask()."""
        ws_url = self._ws_url(conv_id)
        connect_kwargs = {
            "close_timeout": 10,
            "max_size": MAX_WS_MESSAGE,
            "additional_headers": self._headers,
        }
        ssl_context = self._ssl_context(ws_url)
        if ssl_context is not None:
            connect_kwargs["ssl"] = ssl_context

        try:
            ws = await asyncio.wait_for(
                _ws_connect(ws_url, connect_kwargs), timeout=CONNECT_TIMEOUT,
            )
        except asyncio.TimeoutError:
            die(f"timed out connecting to {ws_url}")
        except (OSError, ssl.SSLError, websockets.exceptions.WebSocketException,
                websockets.exceptions.InvalidStatus) as exc:
            die(f"cannot open WebSocket {ws_url}: {exc}")

        try:
            # The server greets us with a system message before accepting input.
            try:
                greeting = await asyncio.wait_for(ws.recv(), timeout=15)
                self.report("connected", level="detail")
                greeting_data = _decode(greeting)
            except asyncio.TimeoutError:
                greeting_data = None
            yield _Session(self, ws, conv_id, greeting_data)
        finally:
            await ws.close()

    async def ask(self, query):
        """One-shot helper: fresh conversation, single question."""
        conv_id = self.create_conversation()
        async with self.session(conv_id) as chat:
            return await chat.ask(query)


class _Session:
    """A live WebSocket conversation. Reuse it for follow-up questions."""

    def __init__(self, client, ws, conv_id, greeting=None):
        self._client = client
        self._ws = ws
        self.conv_id = conv_id
        self.greeting = greeting

    async def ask(self, query):
        client = self._client
        result = QueryResult(query)
        result.conv_id = self.conv_id
        if self.greeting is not None:
            result.messages.append(self.greeting)
            self.greeting = None

        await self._ws.send(json.dumps({"type": "message", "content": query}))
        result.start_time = time.time()
        client.report(f"sent question ({len(query)} chars), waiting for the "
                      "assistant...", level="detail")

        chunks = []
        deadline = result.start_time + client.timeout

        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                result.errors.append(f"Timed out after {client.timeout}s")
                client.report(f"timed out after {client.timeout}s",
                              level="warn")
                break
            try:
                raw = await asyncio.wait_for(self._ws.recv(), timeout=remaining)
            except asyncio.TimeoutError:
                result.errors.append(f"Timed out after {client.timeout}s")
                client.report(f"timed out after {client.timeout}s",
                              level="warn")
                break
            except websockets.exceptions.ConnectionClosed as exc:
                result.errors.append(f"Connection closed by server: {exc}")
                client.report(f"connection closed: {exc}", level="warn")
                break

            data = _decode(raw)
            result.messages.append(data)
            if not isinstance(data, dict):
                continue

            if self._handle(data, result, chunks):
                break

        if not result.answer and chunks:
            result.answer = "".join(chunks)
        result.end_time = time.time()
        return result

    def _handle(self, data, result, chunks):
        """Process one server message. Returns True when the turn is done."""
        report = self._client.report
        msg_type = data.get("type", "")

        if msg_type == "thinking":
            report("thinking...", level="progress")

        elif msg_type == "stream":
            chunks.append(data.get("content", ""))

        elif msg_type == "tool_call":
            call = {
                "tool_name": data.get("tool_name"),
                "tool_kwargs": data.get("tool_kwargs"),
                "tool_id": data.get("tool_id"),
            }
            result.tool_calls.append(call)
            report(f"tool call: {call['tool_name']}", level="progress")

        elif msg_type == "tool_call_result":
            result.tool_results.append({
                "tool_name": data.get("tool_name"),
                "tool_output": data.get("tool_output"),
                "tool_id": data.get("tool_id"),
            })

        elif msg_type == "query_tool_progress":
            label = data.get("query_type", "query")
            status = data.get("status", "running")
            pct = data.get("progress")
            if isinstance(pct, (int, float)):
                report(f"{label}: {status} ({pct:.0f}%)", level="progress")
            else:
                report(f"{label}: {status}", level="progress")

        elif msg_type == "error":
            message = data.get("content", "") or "unknown error"
            result.errors.append(message)
            report(f"server error: {message}", level="warn")

        elif msg_type == "message":
            result.answer = data.get("content", "")
            return True

        elif msg_type == "stop":
            result.stop_reason = data.get("reason", "unknown")
            report(f"stopped: {result.stop_reason}", level="warn")
            return True

        return False


async def _ws_connect(url, kwargs):
    """Connect, tolerating older websockets releases (<14)."""
    try:
        return await websockets.connect(url, **kwargs)
    except TypeError as exc:
        if "additional_headers" not in str(exc):
            raise
        legacy = dict(kwargs)
        legacy["extra_headers"] = legacy.pop("additional_headers")
        return await websockets.connect(url, **legacy)


def _decode(raw):
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return {"type": "unparsed", "content": str(raw)}


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------

class Reporter:
    """Prints progress to stderr so stdout stays clean for the answer."""

    LEVELS = {"quiet": 0, "normal": 1, "verbose": 2}
    ICONS = {"progress": "  ·", "detail": "  ·", "warn": "  !"}

    def __init__(self, mode="normal"):
        self.threshold = self.LEVELS.get(mode, 1)

    def __call__(self, message, level="progress"):
        # Warnings always print -- even --quiet should not hide a server error.
        needed = {"warn": 0, "detail": 2}.get(level, 1)
        if self.threshold >= needed:
            print(f"{self.ICONS.get(level, '  ·')} {message}",
                  file=sys.stderr, flush=True)

    def heading(self, message):
        if self.threshold >= 1:
            print(f"\n=== {message}", file=sys.stderr, flush=True)


CHART_BLOCK_RE = re.compile(r"\n?<chart>\s*(.*?)\s*</chart>\n?",
                            re.DOTALL | re.IGNORECASE)


def strip_chart_blocks(text):
    """Collapse <chart>{...}</chart> blocks the web UI renders as graphs.

    The assistant embeds ECharts JSON for the LogZilla UI to draw. In a
    terminal that is just noise, so replace each block with a one-line marker.
    The full text is always preserved in --json and in saved transcripts.
    """
    def replace(match):
        title = ""
        try:
            spec = json.loads(match.group(1))
            raw_title = spec.get("title")
            if isinstance(raw_title, dict):
                title = raw_title.get("text", "") or ""
            elif isinstance(raw_title, str):
                title = raw_title
        except (ValueError, TypeError, AttributeError):
            pass
        label = f"chart omitted: {title}" if title else "chart omitted"
        return f"\n[{label} — see --json or --keep-charts for the data]\n"

    return CHART_BLOCK_RE.sub(replace, text)


def print_answer(result, reporter, keep_charts=False):
    """Print the answer to stdout, with a short footer on stderr."""
    answer = result.answer or "(no answer returned)"
    if not keep_charts:
        answer = strip_chart_blocks(answer)
    # Flush before the stderr footer so the two streams stay in order when
    # the caller pipes them together (2>&1).
    print(answer, flush=True)
    if reporter.threshold < 1:
        return
    bits = [f"{result.duration_str}"]
    if result.tool_calls:
        names = ", ".join(
            sorted({str(c["tool_name"]) for c in result.tool_calls})
        )
        bits.append(f"{len(result.tool_calls)} tool call(s): {names}")
    if result.errors:
        bits.append(f"{len(result.errors)} error(s)")
    print(f"\n[{' | '.join(bits)}]", file=sys.stderr, flush=True)


def render_markdown(result):
    asked = (
        datetime.fromtimestamp(result.start_time, tz=timezone.utc)
        .strftime("%Y-%m-%d %H:%M:%S UTC")
        if result.start_time else "unknown"
    )
    lines = [
        "# LogZilla AI Query",
        "",
        f"- **Asked:** {asked}",
        f"- **Duration:** {result.duration_str}",
        f"- **Tool calls:** {len(result.tool_calls)}",
        f"- **Errors:** {len(result.errors)}",
        f"- **Conversation ID:** {result.conv_id}",
        "",
        "## Question",
        "",
        "```text",
        result.query.strip(),
        "```",
        "",
        "## Answer",
        "",
        result.answer or "_(no answer returned)_",
        "",
    ]
    if result.tool_calls:
        lines += ["## Tool Calls", ""]
        for i, call in enumerate(result.tool_calls, 1):
            lines += [
                f"### {i}. {call['tool_name']}",
                "",
                "```json",
                json.dumps(call["tool_kwargs"], indent=2, default=str),
                "```",
                "",
            ]
    if result.errors:
        lines += ["## Errors", ""]
        lines += [f"- {err}" for err in result.errors]
        lines.append("")
    return "\n".join(lines)


def slugify(text, max_len=48):
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return (slug[:max_len].rstrip("-") or "query")


def output_dir(save_dir, tag=None):
    """save_dir, or save_dir/tag with --tag, created on first use."""
    path = Path(save_dir).expanduser()
    if tag:
        path = path / tag
    path.mkdir(parents=True, exist_ok=True)
    return path


def result_base(result, save_dir, tag=None):
    """The path every file of one question shares: <dir>/<timestamp>-<slug>."""
    stamp = (
        datetime.fromtimestamp(result.start_time or time.time(),
                               tz=timezone.utc)
        .strftime("%Y%m%dT%H%M%SZ")
    )
    return output_dir(save_dir, tag) / f"{stamp}-{slugify(result.query)}"


def save_result(result, save_dir, include_raw=False, tag=None):
    """Write <timestamp>-<slug>.json and .md into save_dir (or save_dir/tag).

    UTF-8 is explicit on both writes: answers routinely contain non-ASCII
    (em dashes, accented text), which would raise UnicodeEncodeError under a
    C/POSIX or cp1252 locale.
    """
    base = result_base(result, save_dir, tag)

    json_path = base.with_suffix(".json")
    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(result.to_dict(include_raw=include_raw), handle, indent=2,
                  default=str)

    md_path = base.with_suffix(".md")
    with open(md_path, "w", encoding="utf-8") as handle:
        handle.write(render_markdown(result))

    return json_path, md_path


# --------------------------------------------------------------------------
# Capture: the config deploy, the redacted config copy, the Phoenix trace.
# These are the steps LogZilla's own certification harness wraps around this
# tool, so a customer can capture a run the same way in their own lab.
# --------------------------------------------------------------------------

def redact_config_text(text):
    """The ai.yaml text with every *_KEY, *_TOKEN and API_KEY value replaced
    by <redacted>, line by line, so the saved copy never carries a secret."""
    lines = [SECRET_SETTING_RE.sub(r"\1<redacted>", line)
             for line in text.splitlines()]
    return "\n".join(lines) + "\n"


def read_config_file(path):
    path = Path(path).expanduser()
    if not path.is_file():
        die(f"config file not found: {path}")
    return path, path.read_text(encoding="utf-8")


def tracing_from_config(text):
    """(Phoenix base URL, project name) from TRACING_SERVER_URL and
    TRACING_PROJECT_NAME in the ai.yaml text; None for a missing one."""
    url = project = None
    for line in text.splitlines():
        match = re.match(
            r"^\s*(TRACING_SERVER_URL|TRACING_PROJECT_NAME)\s*:\s*(.+?)\s*$",
            line,
        )
        if not match:
            continue
        value = match.group(2).strip().strip("'\"")
        if match.group(1) == "TRACING_SERVER_URL":
            url = value
        else:
            project = value
    return (phoenix_base(url) if url else None), (project or None)


def phoenix_base(url):
    """Scheme and host of the tracing URL (TRACING_SERVER_URL is the OTLP
    endpoint, http://<host>/v1/traces; GraphQL lives at <base>/graphql)."""
    parsed = urlparse(url if "://" in url else f"http://{url}")
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        die(f"Phoenix URL is not a valid URL: {url!r}")
    return f"{parsed.scheme}://{parsed.netloc}"


def deploy_config(host, config_path, wait_s, reporter):
    """Copy the config to the server as root over SSH, reload the AI
    settings, wait for them to apply. Any failure stops the run with the
    remote's own words: a question asked against a half-applied config is
    not a result worth keeping."""
    hostname = urlparse(host).hostname
    steps = [
        ["scp", str(config_path), f"root@{hostname}:{REMOTE_AI_YAML}"],
        ["ssh", f"root@{hostname}", "logzilla", "settings", "reload", "ai"],
    ]
    for argv in steps:
        reporter(" ".join(argv), level="detail")
        try:
            proc = subprocess.run(argv, capture_output=True, text=True)
        except OSError as exc:
            die(f"cannot run {argv[0]}: {exc}")
        if proc.returncode != 0:
            die(f"{argv[0]} failed (exit {proc.returncode}) during the config "
                f"deploy: {proc.stderr.strip() or proc.stdout.strip() or 'no message'}")
    reporter(f"config deployed; waiting {wait_s}s for the settings reload")
    time.sleep(wait_s)


# Phoenix speaks GraphQL at <base>/graphql with no auth. The declarations
# match its schema (node(id: ID!), Project.spans(first: Int!),
# getProjectByName(name: String!)); a wrong one is a validation error.
PHOENIX_PROJECT_QUERY = (
    "query ProjectByName($name: String!) { getProjectByName(name: $name) "
    "{ id name } }"
)
PHOENIX_ROOTS_QUERY = (
    "query Roots($id: ID!, $first: Int!, $start: DateTime, $end: DateTime) { "
    "node(id: $id) { ... on Project { "
    "spans(first: $first, timeRange: {start: $start, end: $end}, "
    'sort: {col: startTime, dir: desc}, filterCondition: "parent_id is None") '
    "{ edges { node { context { traceId } startTime parentId name } } } } } }"
)
PHOENIX_TRACE_QUERY = (
    "query TraceSpans($traceId: String!, $first: Int!) { "
    "getTraceByOtelId(traceId: $traceId) { "
    "traceId startTime endTime latencyMs numSpans spans(first: $first) { "
    "edges { node { id name spanKind statusCode statusMessage startTime "
    "endTime latencyMs parentId context { traceId spanId } "
    "input { value } output { value } "
    "tokenCountTotal tokenCountPrompt tokenCountCompletion } } } } }"
)


def phoenix_graphql(base, query, variables, verify_tls=True):
    url = f"{base}/graphql"
    try:
        resp = requests.post(url, json={"query": query, "variables": variables},
                             timeout=CONNECT_TIMEOUT, verify=verify_tls)
    except requests.exceptions.RequestException as exc:
        die(f"cannot reach Phoenix at {url}: {exc}")
    if resp.status_code != 200:
        die(f"Phoenix {url} answered HTTP {resp.status_code}: {resp.text[:200]}")
    try:
        doc = resp.json()
    except ValueError:
        die(f"Phoenix {url}: the answer is not JSON")
    if doc.get("errors"):
        die(f"Phoenix {url}: {doc['errors']}")
    return doc.get("data") or {}


def phoenix_project_id(base, name, verify_tls=True):
    data = phoenix_graphql(base, PHOENIX_PROJECT_QUERY, {"name": name},
                           verify_tls)
    node = data.get("getProjectByName")
    if isinstance(node, dict) and node.get("id"):
        return str(node["id"])
    die(f"Phoenix {base}: no project named {name!r} (has the server sent any "
        "trace to it yet?)")


def phoenix_export(base, trace_id, verify_tls=True):
    data = phoenix_graphql(base, PHOENIX_TRACE_QUERY,
                           {"traceId": trace_id, "first": TRACE_MAX_SPANS},
                           verify_tls)
    trace = data.get("getTraceByOtelId")
    if not isinstance(trace, dict):
        die(f"Phoenix {base}: trace {trace_id} not found")
    num = trace.get("numSpans")
    if isinstance(num, int) and num > TRACE_MAX_SPANS:
        die(f"Phoenix {base}: trace {trace_id} has {num} spans, over the "
            f"{TRACE_MAX_SPANS} cap")
    spans = [edge["node"] for edge in (trace.get("spans") or {}).get("edges") or []
             if edge.get("node")]
    head = {key: trace.get(key)
            for key in ("traceId", "startTime", "endTime", "latencyMs", "numSpans")}
    return {"trace": head, "spans": spans}


def _utc_iso(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _strings_of(value):
    """Every string inside a span value: the value itself, and when it is
    JSON, every string nested in it (the question sits inside the rendered
    messages as a JSON string, escapes and all)."""
    out = []
    if isinstance(value, str):
        out.append(value)
        try:
            parsed = json.loads(value)
        except ValueError:
            return out
        if isinstance(parsed, (dict, list)):
            out.extend(_strings_of(parsed))
    elif isinstance(value, dict):
        for item in value.values():
            out.extend(_strings_of(item))
    elif isinstance(value, list):
        for item in value:
            out.extend(_strings_of(item))
    return out


def find_trace(base, project_id, result, verify_tls=True):
    """The one Phoenix trace of this question: a root span that started
    inside the question's window and whose spans carry the question text.
    Returns (status, trace_ids, trace): 'exported' with the trace, 'missing'
    with nothing, or 'ambiguous' with the candidates. It never guesses."""
    start = result.start_time - TRACE_WINDOW_SLACK_S
    end = (result.end_time or time.time()) + TRACE_WINDOW_SLACK_S
    data = phoenix_graphql(
        base, PHOENIX_ROOTS_QUERY,
        {"id": project_id, "first": 200, "start": _utc_iso(start),
         "end": _utc_iso(end)},
        verify_tls,
    )
    edges = ((data.get("node") or {}).get("spans") or {}).get("edges") or []
    candidates = []
    for edge in edges:
        node = edge.get("node") or {}
        when = node.get("startTime")
        if node.get("parentId") is not None or not isinstance(when, str):
            continue
        started = datetime.fromisoformat(when.replace("Z", "+00:00")).timestamp()
        if not (start <= started <= end):
            continue
        trace_id = (node.get("context") or {}).get("traceId")
        if trace_id and str(trace_id) not in candidates:
            candidates.append(str(trace_id))
    if len(candidates) > TRACE_MAX_CANDIDATES:
        return "ambiguous", sorted(candidates), None
    needle = " ".join(result.query.split())
    matches = []
    for trace_id in candidates:
        trace = phoenix_export(base, trace_id, verify_tls)
        texts = []
        for span in trace["spans"]:
            for key in ("input", "output"):
                texts.extend(_strings_of((span.get(key) or {}).get("value")))
        if any(needle in " ".join(text.split()) for text in texts):
            matches.append((trace_id, trace))
    if len(matches) == 1:
        return "exported", [matches[0][0]], matches[0][1]
    if not matches:
        return "missing", [], None
    return "ambiguous", sorted(trace_id for trace_id, _trace in matches), None


def trace_path_for(base):
    return base.with_name(base.name + "-trace.json")


def save_trace(trace, path):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(trace, handle, indent=2, default=str)
    return path


# --------------------------------------------------------------------------
# Query sources
# --------------------------------------------------------------------------

def collect_queries(args):
    """Return the list of questions to ask, in order."""
    queries = []

    for raw in args.query:
        if raw == "-":
            queries.extend(_read_stdin_queries())
        elif raw.strip():
            queries.append(raw.strip())

    for path in args.file or []:
        text = _read_query_file(path)
        if text:
            queries.append(text)

    if not queries and not sys.stdin.isatty():
        queries.extend(_read_stdin_queries())

    return queries


def _read_stdin_queries():
    text = sys.stdin.read().strip()
    if not text:
        return []
    # Blank lines separate questions; a single block is one question.
    blocks = [b.strip() for b in re.split(r"\n\s*\n", text)]
    return [b for b in blocks if b]


def _read_query_file(path):
    path = Path(path).expanduser()
    if not path.is_file():
        die(f"question file not found: {path}")
    return path.read_text(encoding="utf-8").strip()


# --------------------------------------------------------------------------
# Modes
# --------------------------------------------------------------------------

async def run_queries(client, queries, args, reporter):
    """Ask each question; returns the list of results."""
    results = []
    total = len(queries)
    same_conversation = args.same_conversation and total > 1

    if same_conversation:
        conv_id = client.create_conversation()
        async with client.session(conv_id) as chat:
            for i, query in enumerate(queries, 1):
                if total > 1:
                    reporter.heading(f"Question {i}/{total}: "
                                     f"{_preview(query)}")
                results.append(await chat.ask(query))
                _emit(results[-1], args, reporter, multiple=total > 1)
        return results

    for i, query in enumerate(queries, 1):
        if total > 1:
            reporter.heading(f"Question {i}/{total}: {_preview(query)}")
        conv_id = client.create_conversation()
        async with client.session(conv_id) as chat:
            result = await chat.ask(query)
        results.append(result)
        _emit(result, args, reporter, multiple=total > 1)

    return results


async def run_interactive(client, args, reporter):
    """Multi-turn REPL over a single conversation."""
    conv_id = client.create_conversation()
    print("LogZilla AI — interactive mode. Type your question and press "
          "Enter.", file=sys.stderr)
    print("Commands: /new (fresh conversation), /quit (exit). Ctrl-C aborts.",
          file=sys.stderr)

    results = []
    while True:
        async with client.session(conv_id) as chat:
            while True:
                try:
                    # input()'s own prompt argument goes to stdout, which would
                    # mix shell chrome into the answers when stdout is
                    # redirected. Keep the prompt on stderr with the rest of
                    # the progress output.
                    print("\nlz-ai> ", end="", file=sys.stderr, flush=True)
                    query = input().strip()
                except (EOFError, KeyboardInterrupt):
                    print(file=sys.stderr)
                    return results
                if not query:
                    continue
                if query in ("/quit", "/exit", "/q"):
                    return results
                if query == "/new":
                    conv_id = client.create_conversation()
                    break  # reopen the session with the new conversation
                try:
                    result = await chat.ask(query)
                except KeyboardInterrupt:
                    print("\n  ! aborted", file=sys.stderr)
                    continue
                results.append(result)
                _emit(result, args, reporter, multiple=False)
                if result.errors and not result.answer:
                    # A dead socket cannot be reused; start a new session.
                    break


def _preview(query, width=70):
    single_line = " ".join(query.split())
    return (single_line[:width] + "…") if len(single_line) > width \
        else single_line


def _emit(result, args, reporter, multiple):
    """Print and optionally persist one result."""
    trace = None
    if args.export_trace and result.start_time:
        status, trace_ids, trace = find_trace(
            args.phoenix_base, args.phoenix_project_id, result,
            verify_tls=not args.insecure,
        )
        base = result_base(result, args.save_dir, args.tag)
        result.trace = {
            "status": status,
            "trace_ids": trace_ids,
            "path": str(trace_path_for(base)) if trace is not None else None,
        }
        if trace is None:
            detail = ", ".join(trace_ids) if trace_ids else "no root span in the window"
            result.errors.append(f"trace {status}: {detail}")
            reporter(f"  ! trace {status}: {detail}", level="progress")

    if args.json:
        if not multiple:
            print(json.dumps(result.to_dict(include_raw=args.raw), indent=2,
                             default=str))
    else:
        print_answer(result, reporter, keep_charts=args.keep_charts)

    if args.save_dir:
        json_path, md_path = save_result(result, args.save_dir,
                                         include_raw=args.raw, tag=args.tag)
        reporter(f"saved {json_path.name} and {md_path.name}")
        if trace is not None:
            path = save_trace(trace, Path(result.trace["path"]))
            reporter(f"saved {path.name} ({trace['trace']['numSpans']} spans)")


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def build_parser():
    parser = argparse.ArgumentParser(
        prog="lz-ai-query.py",
        description="Ask the LogZilla AI assistant a question and print the "
                    "answer.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s "How many events did we receive in the last hour?"
  %(prog)s "Top 10 hosts by event count today" --json
  %(prog)s --file question.txt --save-dir ./results
  %(prog)s "Which devices stopped sending logs?" --timeout 1800
  echo "Summarize critical events today" | %(prog)s
  %(prog)s --interactive
  %(prog)s --file q.txt --save-dir ./results --tag llama-70b-l40s \\
      --deploy-config ./ai.yaml --export-trace

Configuration (first match wins):
  --host / --token flags
  LZ_HOST / LZ_API_TOKEN environment variables
  .env file in the current directory or next to this script

Exit codes:
  0  all questions answered
  1  configuration or connection problem
  2  a question returned an error, timed out, or produced no answer
        """,
    )

    parser.add_argument(
        "query", nargs="*",
        help='Question(s) to ask. Use "-" to read from stdin. With no '
             "argument, stdin is read when piped.",
    )
    parser.add_argument(
        "-f", "--file", action="append", metavar="PATH",
        help="Read a question from a file (repeatable).",
    )
    parser.add_argument(
        "-i", "--interactive", action="store_true",
        help="Start an interactive multi-turn session.",
    )

    conn = parser.add_argument_group("connection")
    conn.add_argument("--host", metavar="URL",
                      help="LogZilla base URL, e.g. https://logzilla.example.com")
    conn.add_argument("--token", metavar="TOKEN",
                      help="LogZilla API token (prefer LZ_API_TOKEN in .env).")
    conn.add_argument("-k", "--insecure", action="store_true",
                      help="Skip TLS certificate verification (self-signed "
                           "certs).")
    conn.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                      metavar="SECONDS",
                      help=f"Per-question timeout (default: {DEFAULT_TIMEOUT}). "
                           "Complex questions can take several minutes.")

    out = parser.add_argument_group("output")
    out.add_argument("--json", action="store_true",
                     help="Print the full result as JSON instead of prose.")
    out.add_argument("--raw", action="store_true",
                     help="Include raw WebSocket messages in JSON/saved "
                          "output (for troubleshooting).")
    out.add_argument("--save-dir", metavar="DIR",
                     help="Also write a .json and .md file per question here.")
    out.add_argument("--keep-charts", action="store_true",
                     help="Keep the <chart> ECharts JSON blocks the assistant "
                          "emits for the web UI. By default they are collapsed "
                          "to a one-line marker in terminal output (JSON and "
                          "saved files always keep the full text).")
    out.add_argument("--tag", metavar="TAG",
                     help="Write the saved files under <save-dir>/<tag>/, a "
                          "label for the model or hardware under test "
                          "(letters, digits, . _ -). Needs --save-dir.")
    out.add_argument("-q", "--quiet", action="store_true",
                     help="Print only the answer; suppress progress output.")
    out.add_argument("-v", "--verbose", action="store_true",
                     help="Show connection details and extra progress.")

    cap = parser.add_argument_group(
        "capture",
        "The steps LogZilla's certification harness wraps around this tool; "
        "with them a run in your own lab is captured the same way.",
    )
    cap.add_argument("--deploy-config", metavar="PATH",
                     help="Before asking, copy this ai.yaml to "
                          f"{REMOTE_AI_YAML} on the server as root over SSH, "
                          "reload the AI settings and wait --reload-wait "
                          "seconds. With --save-dir, a copy with its secrets "
                          "redacted is saved as ai-config.yaml.")
    cap.add_argument("--reload-wait", type=int, default=RELOAD_WAIT_DEFAULT,
                     metavar="SECONDS",
                     help="Seconds to wait after the settings reload "
                          f"(default: {RELOAD_WAIT_DEFAULT}).")
    cap.add_argument("--export-trace", action="store_true",
                     help="After each answer, find the question's trace in "
                          "Phoenix and save it beside the result as "
                          "<timestamp>-<slug>-trace.json. Needs --save-dir "
                          "and the Phoenix server and project (flags, "
                          "LZ_PHOENIX_URL / LZ_PHOENIX_PROJECT, or the "
                          "--deploy-config file's TRACING_* settings). A "
                          "missing or ambiguous trace is an error for that "
                          "question.")
    cap.add_argument("--phoenix-url", metavar="URL",
                     help="The Phoenix server (the TRACING_SERVER_URL of the "
                          "LogZilla AI settings, scheme and host).")
    cap.add_argument("--phoenix-project", metavar="NAME",
                     help="The Phoenix project (TRACING_PROJECT_NAME).")

    parser.add_argument(
        "--same-conversation", action="store_true",
        help="Ask all questions in one conversation so later questions can "
             "reference earlier answers (default: a fresh conversation each).",
    )

    return parser


def main():
    force_utf8_streams()
    parser = build_parser()
    args = parser.parse_args()

    if args.quiet and args.verbose:
        parser.error("--quiet and --verbose are mutually exclusive")
    if args.interactive and (args.query or args.file):
        parser.error("--interactive cannot be combined with questions or "
                     "--file")
    if args.timeout <= 0:
        parser.error("--timeout must be a positive number of seconds")
    if args.tag and not TAG_RE.match(args.tag):
        parser.error("--tag must be letters, digits, '.', '_' or '-' "
                     "(up to 64 characters, starting with a letter or digit)")
    if args.tag and not args.save_dir:
        parser.error("--tag needs --save-dir")
    if args.export_trace and not args.save_dir:
        parser.error("--export-trace needs --save-dir")
    if args.reload_wait < 0:
        parser.error("--reload-wait must be zero or more seconds")
    config_path = config_text = None
    if args.deploy_config:
        config_path, config_text = read_config_file(args.deploy_config)

    mode = "quiet" if args.quiet else ("verbose" if args.verbose else "normal")
    reporter = Reporter(mode)

    # Resolve what to ask before touching the network, so a usage mistake is
    # reported as a usage mistake rather than a config error.
    queries = [] if args.interactive else collect_queries(args)
    if not args.interactive and not queries:
        parser.print_usage(sys.stderr)
        die("no question given. Pass one as an argument, use --file, pipe it "
            "on stdin, or run with --interactive.")

    host, token = resolve_config(args)
    reporter(f"LogZilla: {host}", level="detail")

    if args.export_trace:
        phoenix_url = args.phoenix_url or _from_env("LZ_PHOENIX_URL")
        phoenix_project = args.phoenix_project or _from_env("LZ_PHOENIX_PROJECT")
        if config_text and not (phoenix_url and phoenix_project):
            url_from_config, project_from_config = tracing_from_config(config_text)
            phoenix_url = phoenix_url or url_from_config
            phoenix_project = phoenix_project or project_from_config
        if not (phoenix_url and phoenix_project):
            die("--export-trace needs the Phoenix server and project: pass "
                "--phoenix-url and --phoenix-project, set LZ_PHOENIX_URL and "
                "LZ_PHOENIX_PROJECT, or give --deploy-config an ai.yaml that "
                "sets TRACING_SERVER_URL and TRACING_PROJECT_NAME")
        args.phoenix_base = phoenix_base(phoenix_url)
        args.phoenix_project_id = phoenix_project_id(
            args.phoenix_base, phoenix_project, verify_tls=not args.insecure)
        reporter(f"Phoenix: {args.phoenix_base}, project {phoenix_project}",
                 level="detail")

    if args.deploy_config:
        deploy_config(host, config_path, args.reload_wait, reporter)
        if args.save_dir:
            copy = output_dir(args.save_dir, args.tag) / "ai-config.yaml"
            copy.write_text(redact_config_text(config_text), encoding="utf-8")
            reporter(f"saved {copy.name} (secrets redacted)")

    client = LogZillaAI(
        host, token,
        verify_tls=not args.insecure,
        timeout=args.timeout,
        reporter=reporter,
    )

    if args.interactive:
        results = asyncio.run(run_interactive(client, args, reporter))
        if not results:
            return 0
    else:
        results = asyncio.run(run_queries(client, queries, args, reporter))

        if args.json and len(results) > 1:
            print(json.dumps(
                [r.to_dict(include_raw=args.raw) for r in results],
                indent=2, default=str,
            ))

        if len(results) > 1 and not args.json:
            reporter.heading(f"Summary: {len(results)} question(s)")
            for result in results:
                status = "ok " if result.ok else "FAIL"
                reporter(f"[{status}] {result.duration_str:>14}  "
                         f"{_preview(result.query, 50)}", level="progress")

    return 0 if all(r.ok for r in results) else 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)
