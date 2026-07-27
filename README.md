# LogZilla AI Query Tool

`lz-ai-query.py` is a single-file command-line tool for asking the LogZilla AI
assistant a question and getting the answer back. You can run it once from a
shell, use it in a script, or start an interactive session.

The tool connects to your LogZilla server's AI chat API. It creates a
conversation over REST, sends your question over the WebSocket chat channel,
prints progress while the assistant works, and then prints the answer.

## Requirements

- Python 3.8 or newer
- Network access to your LogZilla server
- LogZilla v6.36 or newer with LogZilla's AI enabled
  (Settings > System Settings > AI Copilot)
- A full LogZilla user auth token

## Install

```bash
# 1. (recommended) create a virtual environment
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate

# 2. install dependencies
pip install -r requirements.txt

# 3. configure your server and token
cp .env.example .env
$EDITOR .env
```

### Getting an auth token

Auth tokens are created with the `logzilla` CLI on the LogZilla server itself,
as root or through `sudo`. You cannot create one from the web UI.

```bash
$ sudo logzilla authtoken create
Creating USER token
user-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

The token is the last line of the output. It starts with `user-` and is
followed by a long hex string. The `xxxx…` shown above is a placeholder, not a
real value. The related commands are:

| Command | Purpose |
| --- | --- |
| `logzilla authtoken create` | Create a full user token |
| `logzilla authtoken create -U <user>` | Create one for a specific user |
| `logzilla authtoken list` | List active tokens |
| `logzilla authtoken info <token>` | Show a token's owner and creation date |
| `logzilla authtoken revoke <token>` | Permanently revoke a token |

> Use a full user token. Ingest-only tokens (created with
> `logzilla authtoken create --ingest-only`, and prefixed `ingest-`) only work
> for sending events to `/incoming`, and the API this tool uses rejects them.
> If you pass one, the tool spots the `ingest-` prefix and tells you, instead
> of failing with a confusing auth error.

Tokens do not expire. They stay valid until you revoke them. Keep them private,
since a token grants access to your log data.

### Configuring the tool

The `.env` file needs two values:

```ini
LZ_HOST=http://logzilla.example.com
LZ_API_TOKEN=user-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

`LZ_HOST` is the base URL of your server. Do not add `/api` to it. The tool
appends that on its own, and if you paste a URL that ends in `/api` it strips
the suffix back off instead of building `/api/api/...`. If your LogZilla runs
behind a reverse proxy on a subpath, such as `https://example.com/logzilla`,
include that subpath in `LZ_HOST`. The tool builds both the REST calls and the
chat WebSocket from it.

You do not have to use a `.env` file. You can export the variables in your
shell, or pass `--host` and `--token` on the command line. If you already
export `LOGZILLA_HOST` and `LOGZILLA_API_KEY` for `logzilla query`, those names
work too. The tool checks `LZ_*` first and falls back to `LOGZILLA_*`.

## Usage

```bash
./lz-ai-query.py "How many events did we receive in the last hour?"
```

The answer is printed to stdout and the progress messages go to stderr. Because
the two streams are separate, you can redirect just the answer to a file:

```bash
./lz-ai-query.py "Top 10 hosts by event count today" > answer.txt
```

### More examples

```bash
# Read the question from a file
./lz-ai-query.py --file question.txt

# Pipe the question in
echo "Which devices stopped sending logs?" | ./lz-ai-query.py

# Machine-readable output (answer, timings, tool calls, errors)
./lz-ai-query.py "Summarize critical events today" --json

# Save a .json and a .md transcript of every question
./lz-ai-query.py "Any signs of a brute-force attempt?" --save-dir ./results

# Several questions in one run (each gets a fresh conversation)
./lz-ai-query.py "How many events today?" "Top 5 sending hosts?"

# Several questions in one conversation, so follow-ups have context
./lz-ai-query.py "Top 5 sending hosts?" "Why is the first one so noisy?" \
    --same-conversation

# Interactive session
./lz-ai-query.py --interactive

# Longer timeout for a complex question (default is 600s)
./lz-ai-query.py "Correlate last week's outage with config changes" \
    --timeout 1800

# Self-signed certificate on the LogZilla server
./lz-ai-query.py "How many events today?" --insecure

# Only the answer, nothing else
./lz-ai-query.py "How many events today?" --quiet
```

### Charts

When a question calls for a graph, the assistant embeds an ECharts JSON block
for the LogZilla web UI to render:

```
<chart>{ "title": {...}, "series": [...] }</chart>
```

In a terminal that block is just a wall of JSON, so by default the tool
replaces it with a short marker:

```
[chart omitted: Events by Severity (Last Hour) — see --json or --keep-charts for the data]
```

The full answer is never thrown away. The `--json` output and the saved
transcripts always keep the original text, chart block and all. Pass
`--keep-charts` if you want the block printed inline too.

### Interactive mode

```
$ ./lz-ai-query.py --interactive
LogZilla AI — interactive mode. Type your question and press Enter.
Commands: /new (fresh conversation), /quit (exit). Ctrl-C aborts.

lz-ai>
```

All questions in one interactive session share a conversation, so you can ask
follow-ups like "now break that down by facility". Type `/new` to start a clean
conversation, and `/quit` or Ctrl-D to exit.

## All options

```
positional arguments:
  query                 Question(s) to ask. Use "-" to read from stdin.
                        With no argument, stdin is read when piped.

options:
  -f, --file PATH       Read a question from a file (repeatable).
  -i, --interactive     Start an interactive multi-turn session.
  --same-conversation   Ask all questions in one conversation so later
                        questions can reference earlier answers.

connection:
  --host URL            LogZilla base URL.
  --token TOKEN         LogZilla API token.
  -k, --insecure        Skip TLS certificate verification.
  --timeout SECONDS     Per-question timeout (default: 600).

output:
  --json                Print the full result as JSON instead of prose.
  --raw                 Include raw WebSocket messages in JSON/saved output.
  --save-dir DIR        Also write a .json and .md file per question.
  --keep-charts         Keep the <chart> JSON blocks in terminal output.
  -q, --quiet           Print only the answer.
  -v, --verbose         Show connection details and extra progress.
```

## JSON output

`--json` prints one object per question, and an array when there are several:

```json
{
  "query": "How many events did we receive in the last hour?",
  "answer": "You received 1,284,331 events in the last hour...",
  "conversation_id": 42,
  "asked_at": "2026-07-27T14:03:11Z",
  "duration_s": 18.4,
  "tool_calls": [
    { "tool_name": "query_events", "tool_kwargs": { "...": "..." },
      "tool_id": "call_1" }
  ],
  "tool_results": [],
  "errors": [],
  "stop_reason": null
}
```

To pull just the answer out, use `jq`:

```bash
./lz-ai-query.py "How many events today?" --json | jq -r .answer
```

## Saved transcripts

`--save-dir DIR` writes two files per question into `DIR`:

| File | Contents |
| --- | --- |
| `<timestamp>-<question-slug>.json` | Same structure as `--json` output |
| `<timestamp>-<question-slug>.md` | Readable transcript: question, answer, tool calls, errors |

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Every question was answered |
| `1` | Configuration, connection, or authentication problem |
| `2` | A question returned an error, timed out, or produced no answer |
| `130` | Interrupted with Ctrl-C |

This makes it easy to branch on the result in a script:

```bash
if ./lz-ai-query.py "Any critical events in the last 15 minutes?" -q > out.txt
then
    cat out.txt
else
    echo "AI query failed" >&2
fi
```

## Troubleshooting

If you see **`authentication failed (401)`**, the token is wrong or has been
revoked. Check it against `logzilla authtoken list` on the server, or create a
new one with `sudo logzilla authtoken create`.

If you see **`this is an ingest-only token`**, you passed an `ingest-` token.
Create a full user token with `sudo logzilla authtoken create` and leave off
`--ingest-only`.

If you see **`... returned 404`**, LogZilla's AI is not enabled on that server.
Turn it on under Settings > System Settings > AI Copilot. It needs LogZilla
v6.36 or newer and a configured model provider, so check with your LogZilla
administrator if you are not sure.

If you see **`TLS verification failed`**, the server is using a self-signed or
internally signed certificate. Either add the CA to your system trust store, or
re-run with `--insecure`.

If you see **`cannot reach LogZilla at ...`**, check `LZ_HOST` and make sure the
host and port are reachable from this machine (`curl -I $LZ_HOST`).

If you see **`Timed out after 600s`**, the question needed longer than the
timeout allowed. Raise it with `--timeout 1800`, or narrow the question down to
a shorter time range or fewer hosts.

If the answer looks truncated, the assistant streamed a partial reply and then
stopped. Re-run with `--verbose` to see the tool calls and the stop reason, and
with `--raw --json` to inspect the full message stream.

## Questions that work well

LogZilla's AI can search events in natural language, analyze patterns, look up
system information, and search the documentation. Here are some examples from
the LogZilla docs:

```text
"Show error events from the last hour"
"How many events occurred in the last 24 hours?"
"Show all critical error events from the last hour regarding authentication"
"Display total event count and trend analysis from the last 24 hours"
"Analyze and explain these unusual SSH failure patterns across servers"
"Identify potential security incidents in the last 24 hours"
"What does this error pattern mean?"
"Where can I find information about configuring alerts?"
```

Questions that name a timeframe and some system context get better answers than
broad ones. For longer troubleshooting, start broad and then narrow down with
follow-up questions in `--interactive` or `--same-conversation`.

## Notes

- Every run starts a fresh conversation by default, so answers are not
  influenced by earlier questions. Use `--same-conversation` or `--interactive`
  when you want that context.
- This tool uses the same LogZilla AI backend as the web UI
  (`/api/ai-conversations` and the `/ws/ai-chat/` WebSocket). Those endpoints
  are internal and are not part of LogZilla's documented public API, so they can
  change between releases. If you need a stable, documented interface for
  structured (non-AI) queries, use `logzilla query` or the `/api` query
  endpoints.
- The tool never prints your API token, and it is not written to saved
  transcripts.
- Saved transcripts contain your log data. Store them accordingly.
- Answers come from an AI assistant querying your live log data. Verify anything
  you plan to act on.
