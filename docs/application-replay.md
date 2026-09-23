# Application replay (experimental)

This is a separate protocol, `rigmark-application-replay.1`. It does not replace
RigMark's standard suite or merge into its historical scores. It schedules
independent users, complete answers, think time and follow-ups containing the
actual previous answer. It measures serving behaviour, not answer correctness.
The public examples are small authored exercises, not production traces or
32K workloads. Supply representative, shareable source material for your use case.

## Prepare and inspect

```sh
./rigmark replay --trace examples/replay/sessions-6.json
./rigmark prepare-replay --code examples/replay/queue_example.py \
  --document examples/replay/service_notes.md \
  --context examples/replay/queue_example.py \
  --context examples/replay/service_notes.md \
  --direction sessions --users 6 --output my-trace.json
```

These commands send no requests. Review the trace before running: its complete
input text and provenance are retained. Directions are `short-first`,
`long-first`, and `sessions`. Context is never padded to claim a token depth;
server usage reports actual token counts where available.

## Execute and compare

```sh
./rigmark replay --trace my-trace.json --base-url http://SERVER:8000 \
  --model MODEL --identity metadata.json --run-id control-boot-1 \
  --output results/replay-control-1 --run
./rigmark compare-replay results/replay-control-1 results/replay-candidate-1
```

Use a fresh output directory and run ID per execution. The identity file is a
user-supplied JSON snapshot; this command does not inspect or attest the live
server. Verify model, checkpoint, engine, topology, cache settings and competing
traffic independently. An unchanged file does not prove an unchanged server.
Authentication reads `OPENAI_API_KEY`; the key and endpoint are not written to
the manifest. Inputs, answers and identity metadata are retained: review them
before publishing any receipts.

Arrivals use a common client clock, with no barrier waiting for incumbents to
finish. Follow-up turns wait for the preceding answer and declared think time.
Failures and capped answers remain in the denominator; a failed turn blocks its
own dependent turns. There are no retries, cache flushes or server lifecycle
commands. `--timeout` bounds HTTP stream reads, including trickled headers.
Operating-system DNS resolution and connection address attempts can take longer;
resolve endpoint connectivity before a timed campaign. `--max-dispatch-lag` (default 0.05 s)
is a client scheduling validity bound, not a server latency target.

Optional SLO scoring requires all three second-valued flags:
`--slo-visible`, `--slo-gap`, `--slo-total`. Goodput includes only completed
requests meeting every threshold, with all planned requests in the denominator.
Delivery gaps are client-visible event gaps, not GPU steps or token inter-arrival
times. Missing token timelines stay unknown; SSE chunks are never counted as
tokens. Sample p95/p99 need at least 20/100 observations and are not population
confidence bounds. Review individual requests and achieved overlap, not only
aggregate scores. No semantic output quality panel is supplied.

`run-isolated` adds a shared namespace per run. This reduces accidental reuse
between runs but is not proof of coldness. `natural` preserves shared prefixes.
Neither cache policy promises exact hit rates. Record cached-token counters
when evaluating a cache claim. Use repeated, crossed boots and retain every round;
a single comparison supplies no significance or promotion decision.

The comparison reader verifies saved trace and identity hashes, request coverage,
and summaries recomputed from raw request receipts. Hashes detect inconsistent
receipts; they do not attest genuine measurements. Both arms must share the
trace, runner, request settings, deadline and client/SLO bounds. Models and
identity may differ for an explicitly labelled appliance comparison. Actual
answers and therefore follow-up context can differ.

## Receipt files

- `manifest.json`, `trace.json`, `identity.json`: exact inputs and protocol settings.
- `request-*.json`: each planned turn, actual clocks, output, usage and status.
- `score.json`: overall and category summaries with achieved overlap.
- `terminal.json`: completion/failure and client validity, without automatic retry.

CPU and loopback tests qualify the collector logic only. They do not qualify its
compatibility or measurement overhead on a particular inference server.
